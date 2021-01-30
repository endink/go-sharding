/*
 * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  File author: Anders Xiao
 */

package mysql

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

var shaPasswordCache = &sync.Map{}

// handshakeInfo handshake response information
type handshakeInfo struct {
	CollationID      CollationID
	AuthResponse     []byte
	Salt             []byte
	AuthPlugin       string
	ClientPluginAuth bool
	UseTLS           bool
	TLSVer           string
}

func errorAccessDenied(username string) error {
	return NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%s'", username)
}

func (l Listener) handshake(cc *Conn, enableTLS bool) error {
	// First build and send the server handshake packet.
	slat, err := cc.writeInitialHandshake(enableTLS)
	if err != nil {
		log.Warnf("writeInitialHandshake error, connId: %d, ip: %s, msg: %s, error: %s",
			cc.ConnectionID, cc.RemoteHost(), " send initial handshake error", err.Error())
		return err
	}

	info, err := l.readClientHandshakePacket(cc, true, slat)
	if err != nil {
		log.Warnf("readClientHandshakePacket error, connId: %d, ip: %s, msg: %s, error: %s",
			cc.ConnectionID, cc.RemoteHost(), " send initial handshake error", err.Error())
		return err
	}

	password, ok := l.userProvider.GetPasswordByUser(cc.User)
	if !ok {
		return errorAccessDenied(cc.User)
	}

	err = l.handleClientHandshake(cc, info, password)
	if err != nil {
		return err
	}
	return nil
}

func (l *Listener) readClientHandshakePacketTls(c *Conn, salt []byte) (*handshakeInfo, error) {
	if l.RequireSecureTransport {
		return nil, fmt.Errorf("server does not allow insecure connections, client must use SSL/TLS")
	}
	// Returns copies of the data, so we can recycle the buffer.
	authResponse, err := l.readClientHandshakePacket(c, false, salt)
	if err != nil {
		return nil, fmt.Errorf("can not parse post-SSL client handshake response from %s: %v", c, err)
	}

	if con, ok := c.conn.(*tls.Conn); ok {
		connState := con.ConnectionState()
		tlsVerStr := tlsVersionToString(connState.Version)
		if tlsVerStr != "" {
			//l.telemetry.AddConnCountByTLSVer(tlsVerStr, 1)
			//defer l.telemetry.AddConnCountByTLSVer(tlsVerStr, -1)
			authResponse.UseTLS = true
			authResponse.TLSVer = tlsVerStr
		}
	}
	return authResponse, nil
}

// parseClientHandshakePacket parses the handshake sent by the client.
// Returns the username, auth method, auth data, error.
// The original data is not pointed at, and can be freed.
func (l *Listener) readClientHandshakePacket(c *Conn, firstTime bool, salt []byte) (*handshakeInfo, error) {
	packetHasReleased := false
	data, err := c.readEphemeralPacketDirect()

	defer func() {
		if !packetHasReleased {
			c.recycleReadPacket()
		}
	}()

	if err != nil {
		// Don't log EOF errors. They cause too much spam, same as main read loop.
		//if err != io.EOF {
		//	log.Infof()
		//}
		return nil, fmt.Errorf("cannot read client handshake response from %s: %v, it may not be a valid MySQL client", c, err)
	}

	pos := 0

	// Client flags, 4 bytes.
	clientFlags, pos, ok := readUint32(data, pos)
	if !ok {
		return nil, fmt.Errorf("parseClientHandshakePacket: can't read client flags")
	}
	if clientFlags&CapabilityClientProtocol41 == 0 {
		return nil, fmt.Errorf("parseClientHandshakePacket: only support protocol 4.1")
	}

	// Remember a subset of the capabilities, so we can use them
	// later in the protocol. If we re-received the handshake packet
	// after SSL negotiation, do not overwrite capabilities.
	if firstTime {
		c.Capabilities = clientFlags & (CapabilityClientDeprecateEOF | CapabilityClientFoundRows)
	}

	// set connection capability for executing multi statements
	if clientFlags&CapabilityClientMultiStatements > 0 {
		c.Capabilities |= CapabilityClientMultiStatements
	}

	// Max packet size. Don't do anything with this now.
	// See doc.go for more information.
	_, pos, ok = readUint32(data, pos)
	if !ok {
		return nil, fmt.Errorf("parseClientHandshakePacket: can't read maxPacketSize")
	}

	// Character set. Need to handle it.
	characterSet, pos, ok := readByte(data, pos)
	if !ok {
		return nil, fmt.Errorf("parseClientHandshakePacket: can't read characterSet")
	}
	c.CharacterSet = CollationID(characterSet)

	// 23x reserved zero bytes.
	pos += 23

	// Check for SSL.
	if firstTime && l.TLSConfig.Load() != nil && clientFlags&CapabilityClientSSL > 0 {
		// Need to switch to TLS, and then re-read the packet.
		conn := tls.Server(c.conn, l.TLSConfig.Load().(*tls.Config))
		c.conn = conn
		c.bufferedReader.Reset(conn)
		c.Capabilities |= CapabilityClientSSL

		packetHasReleased = true
		c.recycleReadPacket()

		return l.readClientHandshakePacketTls(c, salt)
	}

	// username
	username, pos, ok := readNullString(data, pos)
	if !ok {
		return nil, fmt.Errorf("parseClientHandshakePacket: can't read username")
	}
	c.User = username

	// auth-response can have three forms.
	var authResponse []byte
	if clientFlags&CapabilityClientPluginAuthLenencClientData != 0 {
		var l uint64
		l, pos, ok = readLenEncInt(data, pos)
		if !ok {
			return nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response variable length")
		}
		authResponse, pos, ok = readBytesCopy(data, pos, int(l))
		if !ok {
			return nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response")
		}

	} else if clientFlags&CapabilityClientSecureConnection != 0 {
		var l byte
		l, pos, ok = readByte(data, pos)
		if !ok {
			return nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response length")
		}

		authResponse, pos, ok = readBytesCopy(data, pos, int(l))
		if !ok {
			return nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response")
		}
	} else {
		a := ""
		a, pos, ok = readNullString(data, pos)
		if !ok {
			return nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response")
		}
		authResponse = []byte(a)
	}

	dbname := ""
	// db name.
	if clientFlags&CapabilityClientConnectWithDB != 0 {
		dbname, pos, ok = readNullString(data, pos)
		if !ok {
			return nil, fmt.Errorf("parseClientHandshakePacket: can't read dbname")
		}
		c.schemaName = dbname
	}

	// authMethod (with default)
	var authMethod string
	clientPluginAuth := clientFlags&CapabilityClientPluginAuth != 0
	if clientPluginAuth {
		authMethod, pos, ok = readNullString(data, pos)
		if !ok {
			return nil, fmt.Errorf("parseClientHandshakePacket: can't read authMethod")
		}
	}

	// The JDBC driver sometimes sends an empty string as the auth method when it wants to use mysql_native_password
	if authMethod == "" {
		authMethod = MysqlNativePassword
	}

	// Decode connection attributes send by the client
	//if clientFlags&CapabilityClientConnAttr != 0 {
	//	if _, _, err := parseConnAttrs(data, pos); err != nil {
	//		log.Warnf("Decode connection attributes send by the client: %v", err)
	//	}
	//}

	info := &handshakeInfo{
		Salt:             salt,
		AuthPlugin:       authMethod,
		ClientPluginAuth: clientPluginAuth,
		AuthResponse:     authResponse,
	}
	return info, nil
}

func (c *Conn) writeInitialHandshake(enableTLS bool) ([]byte, error) {
	capabilities := DefaultCapability
	if enableTLS {
		capabilities |= CapabilityClientSSL
	}

	salt, e := NewSalt()
	if e != nil {
		return nil, e
	}

	length := 1 + //protocol version
		lenNullString(DefaultServerVersion) + //version
		4 + // connection id
		8 + //auth-plugin-data-part-1
		1 + //filter
		2 + // capability flag lower 2 bytes
		1 + //charset
		2 + //status flag
		2 + //capability flag upper 2 bytes
		1 + //server supports CLIENT_PLUGIN_AUTH and CLIENT_SECURE_CONNECTION
		10 + //reserved 10 [00]
		12 + //auth-plugin-data-part-2
		1 + //add \NUL to terminate the string
		lenNullString(DefaultAuthPlugin) //auth plugin name

	data, pos := c.startEphemeralPacketWithHeader(length)

	//min version 10
	pos = writeByte(data, pos, protocolVersion)

	//server version[00]
	pos = writeNullString(data, pos, DefaultServerVersion)

	//connection id
	pos = writeUint32(data, pos, c.ConnectionID)

	//auth-plugin-data-part-1
	pos = writeBytes(data, pos, salt[:8])

	//filter 0x00 byte, terminating the first part of a scramble
	pos = writeZeroes(data, pos, 1)

	//capability flag lower 2 bytes, using default capability here
	pos = writeUint16(data, pos, uint16(capabilities))

	//charset
	pos = writeByte(data, pos, uint8(DefaultCollationID))

	//status
	pos = writeUint16(data, pos, c.StatusFlags)

	// capability flag upper 2 bytes, using default capability here
	pos = writeUint16(data, pos, uint16(capabilities>>16))

	// server supports CLIENT_PLUGIN_AUTH and CLIENT_SECURE_CONNECTION
	pos = writeByte(data, pos, byte(8+12+1))

	//reserved 10 [00]
	pos = writeZeroes(data, pos, 10)

	//auth-plugin-data-part-2
	pos = writeBytes(data, pos, salt[8:])

	// second part of the password cipher [mininum 13 bytes],
	// where len=MAX(13, length of auth-plugin-data - 8)
	// add \NUL to terminate the string
	pos = writeZeroes(data, pos, 1)

	pos = writeNullString(data, pos, DefaultAuthPlugin)

	// Sanity check.
	if pos != len(data) {
		log.Errorf("error building Handshake packet: got %v bytes expected %v", pos, len(data))
	}

	if err := c.writeEphemeralPacket(); err != nil {
		if strings.HasSuffix(err.Error(), "write: connection reset by peer") {
			return nil, io.EOF
		}
		if strings.HasSuffix(err.Error(), "write: broken pipe") {
			return nil, io.EOF
		}
		return nil, err
	}

	return salt, nil
}

func (l *Listener) compareSha256PasswordAuthData(c *Conn, salt []byte, clientAuthData []byte, password string) error {

	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if password == "" {
			return nil
		}
		return errorAccessDenied(c.User)
	}

	l.getTLSConfig()
	tlsConn, isTls := c.conn.(*tls.Conn)
	if isTls {
		if !tlsConn.ConnectionState().HandshakeComplete {
			return errors.New("incomplete TSL handshake")
		}
		// connection is SSL/TLS, client should send plain password
		// deal with the trailing \NUL added for plain text password received
		if l := len(clientAuthData); l != 0 && clientAuthData[l-1] == 0x00 {
			clientAuthData = clientAuthData[:l-1]
		}
		if bytes.Equal(clientAuthData, []byte(password)) {
			return nil
		}
		return errorAccessDenied(c.User)
	} else {
		// client should send encrypted password
		// decrypt
		dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (c.clientTlsConfig.Certificates[0].PrivateKey).(*rsa.PrivateKey), clientAuthData, nil)
		if err != nil {
			return err
		}
		plain := make([]byte, len(password)+1)
		copy(plain, password)
		for i := range plain {
			j := i % len(salt)
			plain[i] ^= salt[j]
		}
		if bytes.Equal(plain, dbytes) {
			return nil
		}
		return errorAccessDenied(c.User)
	}
}

//https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html
func (l *Listener) compareCacheSha2PasswordAuthData(c *Conn, clientAuthData []byte, salt []byte, password string) error {
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if password == "" {
			return nil
		}
		return errorAccessDenied(c.User)
	}
	exceptedData := CalcCachingSha2Password(salt, password)

	//MYSQL 8 method : fast sha2 auth, if fault try full auth
	if bytes.Equal(exceptedData, clientAuthData) {
		// 'fast' auth: write "More data" packet (first byte == 0x01) with the second byte = 0x03
		return c.writeCachingSha2FastAuthSucceed()
	} else { //fallback to sha2 exchange
		var err error
		// the caching of 'caching_sha2_password' in MySQL, see: https://dev.mysql.com/worklog/task/?id=9591
		cached, ok := shaPasswordCache.Load(fmt.Sprintf("%s@%s", c.User, c.LocalAddr()))
		if ok {
			// Scramble validation
			if compareScramble(cached.([]byte), salt, clientAuthData) {
				// 'fast' auth: write "More data" packet (first byte == 0x01) with the second byte = 0x03
				return c.writeCachingSha2FastAuthSucceed()
			}
			return errorAccessDenied(c.User)
		}
		// cache miss, do full auth
		if err = c.writeCachingSha2NeedFullAuth(); err != nil {
			return err
		}

		// AuthMoreData packet already sent, do full auth
		fullAuthData, err := c.readAuthSwitchPacket()
		if err != nil {
			return err
		}

		if err = l.handleCachingSha2PasswordFullAuth(c, fullAuthData, salt, password); err != nil {
			return err
		}
		l.writeCachingSha2Cache(c, password)
		return nil
	}
}

func (l *Listener) handleClientHandshake(c *Conn, info *handshakeInfo, password string) error {

	if len(info.AuthResponse) == 0 && password == "" {
		return nil
	}

	if info.AuthPlugin != DefaultAuthPlugin && info.ClientPluginAuth {
		if err := c.writeAuthSwitchRequest(DefaultAuthPlugin, info.Salt); err != nil {
			return err
		}
		info.AuthPlugin = DefaultAuthPlugin
		err := l.readAuthSwitchPacketToFill(c, info)
		if err != nil {
			return err
		}
		return l.handleClientHandshake(c, info, password)
	}

	switch info.AuthPlugin {
	case MysqlNativePassword:
		if !bytes.Equal(CalcMySqlNativePassword(info.Salt, []byte(password)), info.AuthResponse) {
			return errorAccessDenied(c.User)
		}
		return nil

	case MysqlCachingSha2Password:
		return l.compareCacheSha2PasswordAuthData(c, info.AuthResponse, info.Salt, password)
	case MysqlSha256Password:
		err := l.handlePublicKeyRetrieval(c, info, info.AuthResponse)
		if err != nil {
			return err
		}
		return l.compareSha256PasswordAuthData(c, info.Salt, info.AuthResponse, password)

	default:
		return fmt.Errorf("unknown authentication plugin name '%s'", info.AuthPlugin)
	}
}

func (c *Conn) readAuthMoreData() ([]byte, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}
	ln := len(data)
	if ln > 0 && data[0] == MoreDataPacket {
		if ln > 1 {
			return data[1:], nil
		}
		return make([]byte, 0), nil
	}
	flag := "<null>"
	if ln > 0 {
		flag = fmt.Sprintf("%x", data[0])
	}
	return nil, fmt.Errorf("excepted is more data packet ( flag: %x ), but got %s", MoreDataPacket, flag)
}

func (l *Listener) readAuthSwitchPacketToFill(c *Conn, info *handshakeInfo) error {
	authData, err := c.readAuthSwitchPacket()
	info.AuthResponse = authData
	if err != nil {
		return err
	}
	return nil
}

// Public Key Retrieval
// See: https://dev.mysql.com/doc/internals/en/public-key-retrieval.html
func (l *Listener) handlePublicKeyRetrieval(c *Conn, info *handshakeInfo, authData []byte) error {
	// if the client use 'sha256_password' auth method, and request for a public key
	// we send back a keyfile with Protocol::AuthMoreData
	if len(authData) == 1 && authData[0] == Sha256RequestPublicKeyPacket {
		if c.Capabilities&CapabilityClientSSL == 0 {
			return errors.New("server does not support SSL: CLIENT_SSL not enabled")
		}
		cfg, ok := l.getTLSConfig()
		if !ok {
			return errors.New("server does not support SSL: CLIENT_SSL not enabled")
		}

		keyBytes, err := genPublicKeyFromTlsConfig(cfg)
		if err != nil {
			return errors.New("marshal rsa public key fault")
		}

		if err = c.writePublicKey(keyBytes); err != nil {
			return err
		}

		if err = l.readAuthSwitchPacketToFill(c, info); err != nil {
			return err
		}
	}
	return nil
}

func (l *Listener) handleCachingSha2PasswordFullAuth(c *Conn, authData []byte, salt []byte, password string) error {
	if len(authData) == 0 {
		return errorAccessDenied(c.User)
	}

	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		if !tlsConn.ConnectionState().HandshakeComplete {
			return errors.New("incomplete TSL handshake")
		}
		// connection is SSL/TLS, client should send plain password
		// deal with the trailing \NUL added for plain text password received
		if length := len(authData); length != 0 && authData[length-1] == 0x00 {
			authData = authData[:length-1]
		}
		if bytes.Equal(authData, []byte(password)) {
			return nil
		}
		return errorAccessDenied(c.User)
	} else {
		// client either request for the public key or send the encrypted password
		if len(authData) == 1 && authData[0] == CacheSha2RequestPublicKeyPacket {
			// send the public key
			if err := c.writePublicKey(l.serverConfig.PublicKey); err != nil {
				return err
			}
			// read the encrypted password
			var err error
			if authData, err = c.readAuthSwitchPacket(); err != nil {
				return err
			}
		}
		// the encrypted password
		// decrypt
		dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (l.serverConfig.Config.Certificates[0].PrivateKey).(*rsa.PrivateKey), authData, nil)
		if err != nil {
			return err
		}
		plain := make([]byte, len(password)+1)
		copy(plain, password)
		for i := range plain {
			j := i % len(salt)
			plain[i] ^= salt[j]
		}
		if bytes.Equal(plain, dbytes) {
			return nil
		}
		return errorAccessDenied(c.User)
	}
}

func (l *Listener) writeCachingSha2Cache(c *Conn, password string) {
	// write cache
	if password == "" {
		return
	}
	m2 := generateScramble(password)
	// caching_sha2_password will maintain an in-memory hash of `user`@`host` => SHA256(SHA256(PASSWORD))

	shaPasswordCache.Store(fmt.Sprintf("%s@%s", c.User, c.conn.LocalAddr()), m2)
}
