/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"crypto/tls"
	"fmt"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/util"
	"github.com/XiaoMi/Gaea/util/netutil"
	"github.com/XiaoMi/Gaea/util/sync2"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// DefaultServerVersion is the default server version we're sending to the client.
	// Can be changed.

	// timing metric keys
	versionTLS10      = "TLS10"
	versionTLS11      = "TLS11"
	versionTLS12      = "TLS12"
	versionTLS13      = "TLS13"
	versionTLSUnknown = "UnknownTLSVersion"
	versionNoTLS      = "None"
)

// A Handler is an interface used by Listener to send queries.
// The implementation of this interface may store data in the ClientData
// field of the Connection for its own purposes.
//
// For a given Connection, all these methods are serialized. It means
// only one of these methods will be called concurrently for a given
// Connection. So access to the Connection ClientData does not need to
// be protected by a mutex.
//
// However, each connection is using one go routine, so multiple
// Connection objects can call these concurrently, for different Connections.
type Handler interface {
	// NewConnection is called when a connection is created.
	// It is not established yet. The handler can decide to
	// set StatusFlags that will be returned by the handshake methods.
	// In particular, ServerStatusAutocommit might be set.
	NewConnection(c *Conn)

	// ConnectionClosed is called when a connection is closed.
	ConnectionClosed(c *Conn)

	// ComQuery is called when a connection receives a query.
	// Note the contents of the query slice may change after
	// the first call to callback. So the Handler should not
	// hang on to the byte slice.
	ComQuery(c *Conn, query string, callback func(*types.Result) error) error

	// ComPrepare is called when a connection receives a prepared
	// statement query.
	ComPrepare(c *Conn, query string, bindVars map[string]*types.BindVariable) ([]*types.Field, error)

	// ComStmtExecute is called when a connection receives a statement
	// execute query.
	ComStmtExecute(c *Conn, prepare *PrepareData, callback func(*types.Result) error) error

	// WarningCount is called at the end of each query to obtain
	// the value to be returned to the client in the EOF packet.
	// Note that this will be called either in the context of the
	// ComQuery callback if the result does not contain any fields,
	// or after the last ComQuery call completes.
	WarningCount(c *Conn) uint16

	ComResetConnection(c *Conn)
}

// Listener is the MySQL server protocol listener.
type Listener struct {
	userProvider UserProvider

	// handler is the data handler.
	handler Handler

	// This is the main listener socket.
	listener net.Listener

	// The following parameters are read by multiple connection go
	// routines.  They are not protected by a mutex, so they
	// should be set after NewListener, and not changed while
	// Accept is running.

	// ServerVersion is the version we will advertise.
	ServerVersion string

	// TLSConfig is the server TLS config. If set, we will advertise
	// that we support SSL.
	// atomic value stores *tls.Config
	TLSConfig atomic.Value

	// AllowClearTextWithoutTLS needs to be set for the
	// mysql_clear_password authentication method to be accepted
	// by the server when TLS is not in use.
	AllowClearTextWithoutTLS sync2.AtomicBool

	// SlowConnectWarnThreshold if non-zero specifies an amount of time
	// beyond which a warning is logged to identify the slow connection
	SlowConnectWarnThreshold sync2.AtomicDuration

	// The following parameters are changed by the Accept routine.

	// Incrementing ID for connection id.
	connectionID uint32

	// Read timeout on a given connection
	connReadTimeout time.Duration
	// Write timeout on a given connection
	connWriteTimeout time.Duration
	// connReadBufferSize is size of buffer for reads from underlying connection.
	// Reads are unbuffered if it's <=0.
	connReadBufferSize int

	// shutdown indicates that Shutdown method was called.
	shutdown sync2.AtomicBool

	// RequireSecureTransport configures the server to reject connections from insecure clients
	RequireSecureTransport bool

	telemetry ConnTelemetry
}

// NewFromListener creares a new mysql listener from an existing net.Listener
func NewFromListener(l net.Listener, handler Handler, connReadTimeout time.Duration, connWriteTimeout time.Duration, userProvider UserProvider) (*Listener, error) {
	cfg := ListenerConfig{
		Listener:           l,
		Handler:            handler,
		ConnReadTimeout:    connReadTimeout,
		ConnWriteTimeout:   connWriteTimeout,
		ConnReadBufferSize: connBufferSize,
	}
	return NewListenerWithConfig(cfg, userProvider)
}

// NewListener creates a new Listener.
func NewListener(protocol, address string, handler Handler, connReadTimeout time.Duration, connWriteTimeout time.Duration, userProvider UserProvider) (*Listener, error) {
	listener, err := net.Listen(protocol, address)
	if err != nil {
		return nil, err
	}

	return NewFromListener(listener, handler, connReadTimeout, connWriteTimeout, userProvider)
}

// ListenerConfig should be used with NewListenerWithConfig to specify listener parameters.
type ListenerConfig struct {
	// Protocol-Address pair and Listener are mutually exclusive parameters
	Protocol           string
	Address            string
	Listener           net.Listener
	Handler            Handler
	ConnReadTimeout    time.Duration
	ConnWriteTimeout   time.Duration
	ConnReadBufferSize int
	MySqlVersion       string
	Telemetry          ConnTelemetry
}

// NewListenerWithConfig creates new listener using provided config. There are
// no default values for config, so caller should ensure its correctness.
func NewListenerWithConfig(cfg ListenerConfig, userProvider UserProvider) (*Listener, error) {
	var l net.Listener
	if cfg.Listener != nil {
		l = cfg.Listener
	} else {
		listener, err := net.Listen(cfg.Protocol, cfg.Address)
		if err != nil {
			return nil, err
		}
		l = listener
	}

	if cfg.Telemetry == nil {
		cfg.Telemetry = NoneConnTelemetry
	}

	return &Listener{
		userProvider:       userProvider,
		handler:            cfg.Handler,
		listener:           l,
		ServerVersion:      cfg.MySqlVersion,
		connectionID:       1,
		connReadTimeout:    cfg.ConnReadTimeout,
		connWriteTimeout:   cfg.ConnWriteTimeout,
		connReadBufferSize: cfg.ConnReadBufferSize,
		telemetry:          cfg.Telemetry,
	}, nil
}

// Addr returns the listener address.
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}

// Accept runs an accept loop until the listener is closed.
func (l *Listener) Accept() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			// Close() was probably called.
			l.telemetry.AddRefuseCount(1)
			return
		}

		acceptTime := time.Now()

		connectionID := l.connectionID
		l.connectionID++

		l.telemetry.AddConnCount(1)
		l.telemetry.AddAcceptCount(1)

		go l.handle(conn, connectionID, acceptTime)
	}
}

func (c *Conn) RemoteHost() string {
	clientHost, _, innerErr := net.SplitHostPort(c.RemoteAddr().String())
	if innerErr != nil {
		log.Warnf("parse host error: %v", innerErr)
		return c.RemoteAddr().String()
	} else {
		// filter lvs detect liveness
		hostname, _ := util.HostName(clientHost)
		if len(hostname) > 0 && strings.Contains(hostname, "lvs") {
			return ""
		}
		return clientHost
	}
}

// handle is called in a go routine for each client connection.
// FIXME(alainjobart) handle per-connection logs in a way that makes sense.
func (l *Listener) handle(conn net.Conn, connectionID uint32, acceptTime time.Time) {
	if l.connReadTimeout != 0 || l.connWriteTimeout != 0 {
		conn = netutil.NewConnWithTimeouts(conn, l.connReadTimeout, l.connWriteTimeout)
	}
	c := newServerConn(conn, l)
	c.ConnectionID = connectionID

	// Catch panics, and close the connection in any case.
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("mysql_server caught panic:\n%v\n%s", x, util.Stack(4))
		}
		// We call endWriterBuffering here in case there's a premature return after
		// startWriterBuffering is called
		_ = c.endWriterBuffering()

		if err := conn.Close(); err != nil {
			log.Warn("close remote client connection fault: ", err.Error())
		}
	}()

	// Tell the handler about the connection coming and going.
	l.handler.NewConnection(c)
	defer l.handler.ConnectionClosed(c)

	// Adjust the count of open connections
	defer l.telemetry.AddConnCount(-1)

	handshakeResult, err := l.handshake(c, false)
	if err != nil {
		c.writeErrorPacketFromError(util.Wrap(err, "server handshake fault"))
	}

	c.User = handshakeResult.User
	c.schemaName = handshakeResult.Database

	if c.User != "" {
		l.telemetry.AddConnCountPerUser(c.User, 1)
		defer l.telemetry.AddConnCountPerUser(c.User, -1)
	}

	// Set initial db name.
	if c.schemaName != "" {
		err = l.handler.ComQuery(c, "use "+EscapeID(c.schemaName), func(result *types.Result) error {
			return nil
		})
		if err != nil {
			c.writeErrorPacketFromError(err)
			return
		}
	}

	// Negotiation worked, send OK packet.
	if err := c.writeOKPacket(&PacketOK{statusFlags: c.StatusFlags}); err != nil {
		log.Errorf("Cannot write OK packet to %s: %v", c, err)
		return
	}

	// Record how long we took to establish the connection
	l.telemetry.RecordConnectTime(acceptTime)

	// Log a warning if it took too long to connect
	connectTime := time.Since(acceptTime)
	if threshold := l.SlowConnectWarnThreshold.Get(); threshold != 0 && connectTime > threshold {
		l.telemetry.AddConnSlow(1)
		log.Warnf("Slow connection from %s: %v", c, connectTime)
	}

	for {
		kontinue := c.handleNextCommand(l.handler)
		if !kontinue {
			return
		}
	}
}

func (l *Listener) getTLSConfig() (*tls.Config, bool) {
	if l.TLSConfig.Load() != nil {
		return l.TLSConfig.Load().(*tls.Config), true
	}
	return nil, false
}

// Close stops the listener, which prevents accept of any new connections. Existing connections won't be closed.
func (l *Listener) Close() {
	l.listener.Close()
}

// Shutdown closes listener and fails any Ping requests from existing connections.
// This can be used for graceful shutdown, to let clients know that they should reconnect to another server.
func (l *Listener) Shutdown() {
	if l.shutdown.CompareAndSwap(false, true) {
		l.Close()
	}
}

func (l *Listener) isShutdown() bool {
	return l.shutdown.Get()
}

func parseConnAttrs(data []byte, pos int) (map[string]string, int, error) {
	var attrLen uint64

	attrLen, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attributes variable length")
	}

	var attrLenRead uint64

	attrs := make(map[string]string)

	for attrLenRead < attrLen {
		var keyLen byte
		keyLen, pos, ok = readByte(data, pos)
		if !ok {
			return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attribute key length")
		}
		attrLenRead += uint64(keyLen) + 1

		var connAttrKey []byte
		connAttrKey, pos, ok = readBytesCopy(data, pos, int(keyLen))
		if !ok {
			return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attribute key")
		}

		var valLen byte
		valLen, pos, ok = readByte(data, pos)
		if !ok {
			return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attribute value length")
		}
		attrLenRead += uint64(valLen) + 1

		var connAttrVal []byte
		connAttrVal, pos, ok = readBytesCopy(data, pos, int(valLen))
		if !ok {
			return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attribute value")
		}

		attrs[string(connAttrKey[:])] = string(connAttrVal[:])
	}

	return attrs, pos, nil

}

// writeAuthSwitchRequest writes an auth switch request packet.
func (c *Conn) writeAuthSwitchRequest(pluginName string, pluginData []byte) error {
	length := 1 + // AuthSwitchRequestPacket
		len(pluginName) + 1 + // 0-terminated pluginName
		len(pluginData) + 1 //end byte 0

	data, pos := c.startEphemeralPacketWithHeader(length)

	// Packet header.
	pos = writeByte(data, pos, AuthSwitchRequestPacket)

	// Copy server version.
	pos = writeNullString(data, pos, pluginName)

	// Copy auth data.
	pos = writeBytes(data, pos, pluginData)

	pos = writeByte(data, pos, 0)
	// Sanity check.
	if pos != len(data) {
		log.Errorf("error building AuthSwitchRequestPacket packet: got %v bytes expected %v", pos, len(data))
	}
	return c.writeEphemeralPacket()
}

// Whenever we move to a new version of go, we will need add any new supported TLS versions here
func tlsVersionToString(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return versionTLS10
	case tls.VersionTLS11:
		return versionTLS11
	case tls.VersionTLS12:
		return versionTLS12
	case tls.VersionTLS13:
		return versionTLS13
	default:
		return versionTLSUnknown
	}
}
