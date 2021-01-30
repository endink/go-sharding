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
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"context"
)

// connectResult is used by Connect.
type connectResult struct {
	c   *Conn
	err error
}

// Connect creates a connection to a server.
// It then handles the initial handshake.
//
// If context is canceled before the end of the process, this function
// will return nil, ctx.Err().
//
// FIXME(alainjobart) once we have more of a server side, add test cases
// to cover all failure scenarios.
func Connect(ctx context.Context, params *ConnParams) (*Conn, error) {
	if params.ConnectTimeoutMs != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(params.ConnectTimeoutMs)*time.Millisecond)
		defer cancel()
	}
	netProto := "tcp"
	addr := ""
	if params.UnixSocket != "" {
		netProto = "unix"
		addr = params.UnixSocket
	} else {
		addr = net.JoinHostPort(params.Host, fmt.Sprintf("%v", params.Port))
	}

	// Figure out the character set we want.
	characterSet, err := parseCharacterSet(params.Charset)
	if err != nil {
		return nil, err
	}

	// Start a background connection routine.  It first
	// establishes a network connection, returns it on the channel,
	// then starts the negotiation, and returns the result on the channel.
	// It can send on the channel, before closing it:
	// - a connectResult with an error and nothing else (when dial fails).
	// - a connectResult with a *Conn and no error, then another one
	//   with possibly an error.
	status := make(chan connectResult)
	go func() {
		defer close(status)
		var err error
		var conn net.Conn

		// Cap the Dial time with the context deadline, plus a
		// few seconds. We want to reclaim resources quickly
		// and not let this go routine stuck in Dial forever.
		//
		// We add a few seconds so we detect the context is
		// Done() before timing out the Dial. That way we'll
		// return the right error to the client (ctx.Err(), vs
		// DialTimeout() error).
		if deadline, ok := ctx.Deadline(); ok {
			timeout := time.Until(deadline) + 5*time.Second
			conn, err = net.DialTimeout(netProto, addr, timeout)
		} else {
			conn, err = net.Dial(netProto, addr)
		}
		if err != nil {
			// If we get an error, the connection to a Unix socket
			// should return a 2002, but for a TCP socket it
			// should return a 2003.
			if netProto == "tcp" {
				status <- connectResult{
					err: NewSQLError(CRConnHostError, SSUnknownSQLState, "net.Dial(%v) failed: %v", addr, err),
				}
			} else {
				status <- connectResult{
					err: NewSQLError(CRConnectionError, SSUnknownSQLState, "net.Dial(%v) to local server failed: %v", addr, err),
				}
			}
			return
		}

		// Send the connection back, so the other side can close it.
		c := newConn(conn)
		c.netProto = netProto
		status <- connectResult{
			c: c,
		}

		// During the handshake, and if the context is
		// canceled, the connection will be closed. That will
		// make any read or write just return with an error
		// right away.
		status <- connectResult{
			err: c.clientHandshake(characterSet, params),
		}
	}()

	// Wait on the context and the status, for the connection to happen.
	var c *Conn
	select {
	case <-ctx.Done():
		// The background routine may send us a few things,
		// wait for them and terminate them properly in the
		// background.
		go func() {
			dialCR := <-status // This one can take a while.
			if dialCR.err != nil {
				// Dial failed, nothing else to do.
				return
			}
			// Dial worked, close the connection, wait for the end.
			// We wait as not to leave a channel with an unread value.
			dialCR.c.Close()
			<-status
		}()
		return nil, ctx.Err()
	case cr := <-status:
		if cr.err != nil {
			// Dial failed, no connection was ever established.
			return nil, cr.err
		}

		// Dial worked, we have a connection. Keep going.
		c = cr.c
	}

	// Wait for the end of the handshake.
	select {
	case <-ctx.Done():
		// We are interrupted. Close the connection, wait for
		// the handshake to finish in the background.
		c.Close()
		go func() {
			// Since we closed the connection, this one should be fast.
			// We wait as not to leave a channel with an unread value.
			<-status
		}()
		return nil, ctx.Err()
	case cr := <-status:
		if cr.err != nil {
			c.Close()
			return nil, cr.err
		}
	}
	return c, nil
}

// Ping implements mysql ping command.
func (c *Conn) Ping() error {
	// This is a new command, need to reset the sequence.
	c.sequence = 0
	data, pos := c.startEphemeralPacketWithHeader(1)
	data[pos] = ComPing

	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerGone, SSUnknownSQLState, "%v", err)
	}
	data, err := c.readEphemeralPacket()
	if err != nil {
		return NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
	}
	defer c.recycleReadPacket()
	switch data[0] {
	case OKPacket:
		return nil
	case ErrPacket:
		return ParseErrorPacket(data)
	}
	return fmt.Errorf("unexpected packet type: %d", data[0])
}

// parseCharacterSet parses the provided character set.
// Returns SQLError(CRCantReadCharset) if it can't.
func parseCharacterSet(cs string) (CollationID, error) {
	// Check if it's empty, return utf8. This is a reasonable default.
	if cs == "" {
		return DefaultCollationID, nil
	}

	// Check if it's in our map.
	characterSet, ok := CollationIds[strings.ToLower(cs)]
	if ok {
		return characterSet, nil
	}

	// As a fallback, try to parse a number. So we support more values.
	if i, err := strconv.ParseInt(cs, 10, 8); err == nil {
		return CollationID(uint8(i)), nil
	}

	// No luck.
	return 0, NewSQLError(CRCantReadCharset, SSUnknownSQLState, "failed to interpret character set '%v'. Try using an integer value if needed", cs)
}

// clientHandshake handles the client side of the handshake.
// Note the connection can be closed while this is running.
// Returns a SQLError.
func (c *Conn) clientHandshake(characterSet CollationID, params *ConnParams) error {
	// Wait for the server initial handshake packet, and parse it.
	data, err := c.readPacket()
	if err != nil {
		return NewSQLError(CRServerLost, "", "initial packet read failed: %v", err)
	}
	capabilities, authPlugin, salt, err := c.parseInitialHandshakePacket(data)
	if err != nil {
		return err
	}
	//c.fillFlavor(params)

	// Sanity check.
	if capabilities&CapabilityClientProtocol41 == 0 {
		return NewSQLError(CRVersionError, SSUnknownSQLState, "cannot connect to servers earlier than 4.1")
	}

	// Remember a subset of the capabilities, so we can use them
	// later in the protocol.
	c.Capabilities = 0
	if !params.DisableClientDeprecateEOF {
		c.Capabilities = capabilities & (CapabilityClientDeprecateEOF)
	}

	// Handle switch to SSL if necessary.
	if params.Flags&CapabilityClientSSL > 0 {
		// If client asked for SSL, but server doesn't support it,
		// stop right here.
		if capabilities&CapabilityClientSSL == 0 {
			return NewSQLError(CRSSLConnectionError, SSUnknownSQLState, "server doesn't support SSL but client asked for it")
		}

		// The ServerName to verify depends on what the hostname is.
		// We use the params's ServerName if specified. Otherwise:
		// - If using a socket, we use "localhost".
		// - If it is an IP address, we need to prefix it with 'IP:'.
		// - If not, we can just use it as is.
		serverName := "localhost"
		if params.ServerName != "" {
			serverName = params.ServerName
		} else if params.Host != "" {
			if net.ParseIP(params.Host) != nil {
				serverName = "IP:" + params.Host
			} else {
				serverName = params.Host
			}
		}

		// Build the TLS config.
		clientConfig, err := ClientTlsConfig(params.SslCert, params.SslKey, params.SslCa, serverName)
		if err != nil {
			return NewSQLError(CRSSLConnectionError, SSUnknownSQLState, "error loading client cert and ca: %v", err)
		}

		// Send the SSLRequest packet.
		if err := c.writeSSLRequest(capabilities, characterSet, params); err != nil {
			return err
		}

		// Switch to SSL.
		conn := tls.Client(c.conn, clientConfig)
		c.conn = conn
		c.bufferedReader.Reset(conn)
		c.Capabilities |= CapabilityClientSSL
	}
	// Password encryption.
	scrambledPassword, err := genClientAuthData(authPlugin, params.Pass, salt, c.IsTLS())

	if err != nil {
		return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "auth data (salt) cant be used to calc password")
	}

	// Client Session Tracking Capability.
	if params.Flags&CapabilityClientSessionTrack == CapabilityClientSessionTrack {
		// If client asked for ClientSessionTrack, but server doesn't support it,
		// stop right here.
		if capabilities&CapabilityClientSessionTrack == 0 {
			return NewSQLError(CRSSLConnectionError, SSUnknownSQLState, "server doesn't support ClientSessionTrack but client asked for it")
		}
		c.Capabilities |= CapabilityClientSessionTrack
	}

	// Build and send our handshake response 41.
	// Note this one will never have SSL flag on.
	if err := c.writeHandshakeResponse41(capabilities, authPlugin, scrambledPassword, characterSet, params); err != nil {
		return err
	}

	err = c.handleServerAuthResponse(params, authPlugin, salt, false)
	if err != nil {
		return err
	}

	// If the server didn't support DbName in its handshake, set
	// it now. This is what the 'mysql' client does.
	if capabilities&CapabilityClientConnectWithDB == 0 && params.DbName != "" {
		// Write the packet.
		if err := c.writeComInitDB(params.DbName); err != nil {
			return err
		}

		// Wait for response, should be OK.
		response, err := c.readPacket()
		if err != nil {
			return NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
		}
		switch response[0] {
		case OKPacket:
			// OK packet, we are authenticated.
			return nil
		case ErrPacket:
			return ParseErrorPacket(response)
		default:
			// FIXME(alainjobart) handle extra auth cases and so on.
			return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "initial server response is asking for more information, not implemented yet: %v", response)
		}
	}

	return nil
}

// parseInitialHandshakePacket parses the initial handshake from the server.
// It returns a SQLError with the right code.
func (c *Conn) parseInitialHandshakePacket(data []byte) (uint32, string, []byte, error) {
	pos := 0

	// Protocol version.
	pver, pos, ok := readByte(data, pos)
	if !ok {
		return 0, "", nil, NewSQLError(CRVersionError, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no protocol version")
	}

	// Server is allowed to immediately send ERR packet
	if pver == ErrPacket {
		errorCode, pos, _ := readUint16(data, pos)
		// Normally there would be a 1-byte sql_state_marker field and a 5-byte
		// sql_state field here, but docs say these will not be present in this case.
		errorMsg, _, _ := readEOFString(data, pos)
		return 0, "", nil, NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "immediate error from server errorCode=%v errorMsg=%v", errorCode, errorMsg)
	}

	if pver != protocolVersion {
		return 0, "", nil, NewSQLError(CRVersionError, SSUnknownSQLState, "bad protocol version: %v", pver)
	}

	// Read the server version.
	c.ServerVersion, pos, ok = readNullString(data, pos)
	if !ok {
		return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no server version")
	}

	// Read the connection id.
	c.ConnectionID, pos, ok = readUint32(data, pos)
	if !ok {
		return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no connection id")
	}

	// Read the first part of the auth-plugin-data
	authPluginData, pos, ok := readBytes(data, pos, 8)
	if !ok {
		return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no auth-plugin-data-part-1")
	}

	// One byte filler, 0. We don't really care about the value.
	_, pos, ok = readByte(data, pos)
	if !ok {
		return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no filler")
	}

	// Lower 2 bytes of the capability flags.
	capLower, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no capability flags (lower 2 bytes)")
	}
	var capabilities = uint32(capLower)

	// The packet can end here.
	if pos == len(data) {
		return capabilities, MysqlNativePassword, authPluginData, nil
	}

	// Character set.
	characterSet, pos, ok := readByte(data, pos)
	if !ok {
		return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no character set")
	}
	c.CharacterSet = CollationID(characterSet)

	// Status flags. Ignored.
	_, pos, ok = readUint16(data, pos)
	if !ok {
		return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no status flags")
	}

	// Upper 2 bytes of the capability flags.
	capUpper, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no capability flags (upper 2 bytes)")
	}
	capabilities += uint32(capUpper) << 16

	// Length of auth-plugin-data, or 0.
	// Only with CLIENT_PLUGIN_AUTH capability.
	var authPluginDataLength byte
	if capabilities&CapabilityClientPluginAuth != 0 {
		authPluginDataLength, pos, ok = readByte(data, pos)
		if !ok {
			return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no length of auth-plugin-data")
		}
	} else {
		// One byte filler, 0. We don't really care about the value.
		_, pos, ok = readByte(data, pos)
		if !ok {
			return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no length of auth-plugin-data filler")
		}
	}

	// 10 reserved 0 bytes.
	pos += 10

	if capabilities&CapabilityClientSecureConnection != 0 {
		// The next part of the auth-plugin-data.
		// The length is max(13, length of auth-plugin-data - 8).
		l := int(authPluginDataLength) - 8
		if l > 13 {
			l = 13
		}
		var authPluginDataPart2 []byte
		authPluginDataPart2, pos, ok = readBytes(data, pos, l)
		if !ok {
			return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no auth-plugin-data-part-2")
		}

		// The last byte has to be 0, and is not part of the data.
		if authPluginDataPart2[l-1] != 0 {
			return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: auth-plugin-data-part-2 is not 0 terminated")
		}
		authPluginData = append(authPluginData, authPluginDataPart2[0:l-1]...)
	}

	var authPluginName string
	// Auth-plugin name.
	if capabilities&CapabilityClientPluginAuth != 0 {
		authPluginName, _, ok = readNullString(data, pos)

		if !ok {
			// Fallback for versions prior to 5.5.10 and
			// 5.6.2 that don't have a null terminated string.
			authPluginName = string(data[pos : len(data)-1])
		}

		if authPluginName != MysqlNativePassword && authPluginName != MysqlCachingSha2Password {
			return 0, "", nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: only support %s auth plugin, but got %v",
				strings.Join([]string{MysqlNativePassword, MysqlCachingSha2Password}, ","), authPluginName)
		}
	}

	return capabilities, authPluginName, authPluginData, nil
}

// writeSSLRequest writes the SSLRequest packet. It's just a truncated
// HandshakeResponse41.
func (c *Conn) writeSSLRequest(capabilities uint32, characterSet CollationID, params *ConnParams) error {
	// Build our flags, with CapabilityClientSSL.
	capabilityFlags := CapabilityFlagsSsl |
		// If the server supported
		// CapabilityClientDeprecateEOF, we also support it.
		c.Capabilities&CapabilityClientDeprecateEOF |
		// Pass-through ClientFoundRows flag.
		CapabilityClientFoundRows&uint32(params.Flags)

	length :=
		4 + // Client capability flags.
			4 + // Max-packet size.
			1 + // Character set.
			23 // Reserved.

	// Add the DB name if the server supports it.
	if params.DbName != "" && (capabilities&CapabilityClientConnectWithDB != 0) {
		capabilityFlags |= CapabilityClientConnectWithDB
	}

	data, pos := c.startEphemeralPacketWithHeader(length)

	// Client capability flags.
	pos = writeUint32(data, pos, capabilityFlags)

	// Max-packet size, always 0. See doc.go.
	pos = writeZeroes(data, pos, 4)

	// Character set.
	_ = writeByte(data, pos, byte(characterSet))

	// And send it as is.
	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerLost, SSUnknownSQLState, "cannot send SSLRequest: %v", err)
	}
	return nil
}

// CapabilityFlags are client capability flag sent to mysql on connect
const CapabilityFlags uint32 = CapabilityClientLongPassword |
	CapabilityClientLongFlag |
	CapabilityClientProtocol41 |
	CapabilityClientTransactions |
	CapabilityClientSecureConnection |
	CapabilityClientMultiStatements |
	CapabilityClientMultiResults |
	CapabilityClientPluginAuth |
	CapabilityClientPluginAuthLenencClientData

// CapabilityFlagsSsl signals that we can handle SSL as well
const CapabilityFlagsSsl = CapabilityFlags |
	CapabilityClientSSL

// writeHandshakeResponse41 writes the handshake response.
// Returns a SQLError.
func (c *Conn) writeHandshakeResponse41(capabilities uint32, authPlugin string, scrambledPassword []byte, characterSet CollationID, params *ConnParams) error {
	// Build our flags.
	capabilityFlags := CapabilityFlags |
		// If the server supported
		// CapabilityClientDeprecateEOF, we also support it.
		c.Capabilities&CapabilityClientDeprecateEOF |
		// Pass-through ClientFoundRows flag.
		CapabilityClientFoundRows&uint32(params.Flags) |
		// If the server supported
		// CapabilityClientSessionTrack, we also support it.
		c.Capabilities&CapabilityClientSessionTrack

	// FIXME(alainjobart) add multi statement.

	length :=
		4 + // Client capability flags.
			4 + // Max-packet size.
			1 + // Character set.
			23 + // Reserved.
			lenNullString(params.Uname) +
			// length of scrambled password is handled below.
			len(scrambledPassword) +
			len(authPlugin) + // "mysql_native_password" string.
			1 // terminating zero.

	// Add the DB name if the server supports it.
	if params.DbName != "" && (capabilities&CapabilityClientConnectWithDB != 0) {
		capabilityFlags |= CapabilityClientConnectWithDB
		length += lenNullString(params.DbName)
	}

	if capabilities&CapabilityClientPluginAuthLenencClientData != 0 {
		length += lenEncIntSize(uint64(len(scrambledPassword)))
	} else {
		length++
	}

	data, pos := c.startEphemeralPacketWithHeader(length)

	// Client capability flags.
	pos = writeUint32(data, pos, capabilityFlags)

	// Max-packet size, always 0. See doc.go.
	pos = writeZeroes(data, pos, 4)

	// Character set.
	pos = writeByte(data, pos, byte(characterSet))

	// 23 reserved bytes, all 0.
	pos = writeZeroes(data, pos, 23)

	// Username
	pos = writeNullString(data, pos, params.Uname)

	// Scrambled password.  The length is encoded as variable length if
	// CapabilityClientPluginAuthLenencClientData is set.
	if capabilities&CapabilityClientPluginAuthLenencClientData != 0 {
		pos = writeLenEncInt(data, pos, uint64(len(scrambledPassword)))
	} else {
		data[pos] = byte(len(scrambledPassword))
		pos++
	}
	pos += copy(data[pos:], scrambledPassword)

	// DbName, only if server supports it.
	if params.DbName != "" && (capabilities&CapabilityClientConnectWithDB != 0) {
		pos = writeNullString(data, pos, params.DbName)
		c.schemaName = params.DbName
	}

	// Assume native client during response
	pos = writeNullString(data, pos, authPlugin)

	// Sanity-check the length.
	if pos != len(data) {
		_ = c.writeEphemeralPacket()
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "writeHandshakeResponse41: only packed %v bytes, out of %v allocated", pos, len(data))
	}

	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerLost, SSUnknownSQLState, "cannot send HandshakeResponse41: %v", err)
	}
	return nil
}

//https://insidemysql.com/preparing-your-community-connector-for-mysql-8-part-2-sha256/
func (c *Conn) handleServerAuthResponse(params *ConnParams, authPlugin string, salt []byte, disableSwitch bool) error {
	// Read the server response.
	response, err := c.readPacket()
	if err != nil {
		return NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
	}
	switch response[0] {
	case OKPacket:
		// OK packet, we are authenticated. Save the user, keep going.
		c.User = params.Uname
	case MoreDataPacket:
		err = c.writeAuthResponse(authPlugin, response[1:], salt, params)
		if err = c.readOk(CRServerHandshakeErr); err != nil {
			return err
		}
	case AuthSwitchRequestPacket:
		// Server is asking to use a different auth method. We
		// only support cleartext plugin.
		authPlugin, salt, err = parseServerAuthSwitchRequest(response)
		if err != nil {
			return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "cannot parse auth switch request: %v", err)
		}
		switch authPlugin {
		case MysqlClearPassword:
			if err = c.writeClearTextPassword(params); err != nil {
				return err
			}
		case MysqlNativePassword, MysqlCachingSha2Password, MysqlSha256Password:
			enc, e := genClientAuthData(authPlugin, params.Pass, salt, c.IsTLS())
			if e != nil {
				return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "auth data (salt) cant be used to calc password")
			}
			err = c.writeEncryptedPassword(enc)
			if err != nil {
				return err
			}
		default:
			return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "server asked for unsupported auth method: %v", authPlugin)
		}

		if err = c.readOk(CRServerHandshakeErr); err == nil {
			c.User = params.Uname
		}
		return err
	case ErrPacket:
		return ParseErrorPacket(response)
	default:
		return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "initial server response cannot be parsed: %v", response)
	}
	return nil
}

func parseServerAuthSwitchRequest(data []byte) (string, []byte, error) {
	pos := 1
	pluginName, pos, ok := readNullString(data, pos)
	if !ok {
		return "", nil, fmt.Errorf("cannot get plugin name from AuthSwitchRequest: %v", data)
	}

	// If this was a request with a salt in it, max 20 bytes
	salt := data[pos:]
	if len(salt) > 20 {
		salt = salt[:20]
	}
	return pluginName, salt, nil
}

func (c *Conn) writeAuthResponse(authPlugin string, data []byte, salt []byte, params *ConnParams) error {
	if len(data) == 0 || (len(data) == 1 && data[0] == CacheSha2FastAuthSucceed) {
		return nil
	}

	switch authPlugin {
	case MysqlCachingSha2Password:
		if len(data) == 1 {
			switch data[0] {
			case CacheSha2FastAuthSucceed:
				return nil
			case CacheSha2FullAuthRequired:
				// need full authentication
				// https://dev.mysql.com/doc/refman/8.0/en/sha256-pluggable-authentication.html
				if c.IsTLS() {
					return c.writeClearTextPassword(params)
				} else {
					return c.writePublicKeyAuthPacket(params.Pass, salt)
				}
			}
		}
		return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "server asked for more auth data, but got known flat: %v, auth plugin: %s", data[0], authPlugin)
	case MysqlSha256Password:
		// for sha256 we will get the public key
		block, _ := pem.Decode(data)
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return err
		}
		enc, err := EncryptPassword(params.Pass, salt, pub.(*rsa.PublicKey))
		if err != nil {
			return err
		}
		// send encrypted password
		err = c.writeEncryptedPassword(enc)
		if err != nil {
			return err
		}
	default:
		return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "server asked for more auth data, but got known flat: %v, auth plugin: %s", data[0], authPlugin)
	}
	return nil
}

// WritePublicKeyAuthPacket: Caching sha2 authentication. Public key request and send encrypted password
// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchResponse
func (c *Conn) writePublicKeyAuthPacket(password string, cipher []byte) error {
	// request public key
	if err := c.writeCachingSha2RequestPubKey(); err != nil {
		return err
	}

	data, err := c.readAuthMoreData()
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return fmt.Errorf("excepted server public key for cachint sha2 authentication, but got 0 bytes")
	}

	block, _ := pem.Decode(data) // 1 byte: more data flag
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return err
	}

	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(cipher)
		plain[i] ^= cipher[j]
	}
	sha1v := sha1.New()
	enc, _ := rsa.EncryptOAEP(sha1v, rand.Reader, pub.(*rsa.PublicKey), plain, nil)

	data, pos := c.startEphemeralPacketWithHeader(len(enc))
	writeBytes(data, pos, enc)
	return c.writeEphemeralPacket()
}

// writeClearTextPassword writes the clear text password.
// Returns a SQLError.
func (c *Conn) writeClearTextPassword(params *ConnParams) error {
	length := len(params.Pass) + 1
	data, pos := c.startEphemeralPacketWithHeader(length)
	pos = writeNullString(data, pos, params.Pass)
	// Sanity check.
	if pos != len(data) {
		log.Errorf("error building ClearTextPassword packet: got %v bytes expected %v", pos, len(data))
	}
	return c.writeEphemeralPacket()
}

func (c *Conn) writeEncryptedPassword(enc []byte) error {
	data, pos := c.startEphemeralPacketWithHeader(len(enc))
	writeBytes(data, pos, enc)
	// Sanity check.
	if pos != len(data) {
		log.Errorf("error building password packet: got %v bytes expected %v", pos, len(data))
	}
	return c.writeEphemeralPacket()
}
