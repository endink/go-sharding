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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/pingcap/parser/mysql"
	"sync"
)

var ErrAccessDenied = errors.New("access denied")
var tlsConfig tls.Config

var ShaPasswordCache = &sync.Map{}

// HandshakeResult handshake response information
type HandshakeResult struct {
	CollationID      CollationID
	User             string
	AuthResponse     []byte
	Salt             []byte
	Database         string
	AuthPlugin       string
	ClientPluginAuth bool
	UseTLS           bool
	TLSVer           string
}

func (l Listener) handshake(cc *Conn, enableTLS bool) (*HandshakeResult, error) {
	// First build and send the server handshake packet.
	slat, err := cc.writeInitialHandshake(enableTLS)
	if err != nil {
		log.Warnf("writeInitialHandshake error, connId: %d, ip: %s, msg: %s, error: %s",
			cc.ConnectionID, cc.RemoteHost(), " send initial handshake error", err.Error())
		return nil, err
	}

	info, err := l.readClientHandshakePacket(cc, true, slat)
	if err != nil {
		log.Warnf("readClientHandshakePacket error, connId: %d, ip: %s, msg: %s, error: %s",
			cc.ConnectionID, cc.RemoteHost(), " send initial handshake error", err.Error())
		return nil, err
	}

	password, ok := l.userProvider.GetPasswordByUser(info.User)
	if !ok {
		return nil, ErrAccessDenied
	}

	err = l.handleClientHandshake(cc, info, password)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (l *Listener) readClientHandshakePacketTls(c *Conn, salt []byte) (*HandshakeResult, error) {
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
func (l *Listener) readClientHandshakePacket(c *Conn, firstTime bool, salt []byte) (*HandshakeResult, error) {
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

	info := &HandshakeResult{
		Salt:             salt,
		User:             username,
		Database:         dbname,
		AuthPlugin:       authMethod,
		ClientPluginAuth: clientPluginAuth,
		AuthResponse:     authResponse,
	}
	return info, nil
}

//func (cc *Conn) readHandshakeResponse(salt []byte) (*HandshakeResult, error) {
//	info := &HandshakeResult{
//		Salt: salt,
//	}
//
//	data, err := cc.readEphemeralPacketDirect()
//	defer cc.recycleReadPacket()
//	if err != nil {
//		return info, err
//	}
//
//	pos := 0
//
//	// Client flags, 4 bytes.
//	var ok bool
//	var capability uint32
//	capability, pos, ok = readUint32(data, pos)
//	if !ok {
//		return info, fmt.Errorf("readHandshakeResponse: can't read client flags")
//	}
//	if capability&mysql.ClientProtocol41 == 0 {
//		return info, fmt.Errorf("readHandshakeResponse: only support protocol 4.1")
//	}
//
//	// Max packet size. Don't do anything with this now.
//	_, pos, ok = readUint32(data, pos)
//	if !ok {
//		return info, fmt.Errorf("readHandshakeResponse: can't read maxPacketSize")
//	}
//
//	// Character set
//	collationID, pos, ok := readByte(data, pos)
//	if !ok {
//		return info, fmt.Errorf("readHandshakeResponse: can't read characterSet")
//	}
//	info.CollationID = CollationID(collationID)
//
//	// reserved 23 zero bytes, skipped
//	pos += 23
//
//	// username
//	var user string
//	user, pos, ok = readNullString(data, pos)
//	if !ok {
//		return info, fmt.Errorf("readHandshakeResponse: can't read username")
//	}
//	info.User = user
//	info.ClientPluginAuth = capability&mysql.ClientPluginAuth > 0
//	info.AuthResponse, pos, ok = readAuthData(data, pos, capability)
//
//	// check if with database
//	if capability&mysql.ClientConnectWithDB > 0 {
//		var db string
//		db, pos, ok = readNullString(data, pos)
//		if !ok {
//			return info, fmt.Errorf("readHandshakeResponse: can't read db")
//		}
//		info.Database = db
//	}
//
//	info.AuthPlugin, _ = readPluginName(data, pos, capability)
//	return info, nil
//}

func (c *Conn) writeInitialHandshake(enableTLS bool) (salt []byte, err error) {
	capabilities := DefaultCapability
	if enableTLS {
		capabilities |= CapabilityClientSSL
	}

	salt, e := NewSalt()
	if e != nil {
		return nil, e
	}

	data := make([]byte, 4)

	//min version 10
	data = append(data, protocolVersion)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0x00)

	//connection id
	data = append(data, byte(c.ConnectionID), byte(c.ConnectionID>>8), byte(c.ConnectionID>>16), byte(c.ConnectionID>>24))

	//auth-plugin-data-part-1
	data = append(data, salt[0:8]...)

	//filter 0x00 byte, terminating the first part of a scramble
	data = append(data, 0x00)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(capabilities), byte(capabilities>>8))
	//charset
	data = append(data, uint8(DefaultCollationID))

	//status
	data = append(data, byte(0), byte(0>>8))

	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DefaultCapability>>16), byte(DefaultCapability>>24))

	// server supports CLIENT_PLUGIN_AUTH and CLIENT_SECURE_CONNECTION
	data = append(data, byte(8+12+1))

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, salt[8:]...)
	// second part of the password cipher [mininum 13 bytes],
	// where len=MAX(13, length of auth-plugin-data - 8)
	// add \NUL to terminate the string
	data = append(data, 0x00)

	// auth plugin name
	data = append(data, DefaultAuthPlugin...)

	// EOF if MySQL version (>= 5.5.7 and < 5.5.10) or (>= 5.6.0 and < 5.6.2)
	// \NUL otherwise, so we use \NUL
	data = append(data, 0)
	if e = c.writePacket(data); e != nil {
		return nil, e
	}
	return salt, nil
}

func (l *Listener) compareSha256PasswordAuthData(c *Conn, salt []byte, clientAuthData []byte, password string) error {

	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if password == "" {
			return nil
		}
		return ErrAccessDenied
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
		return ErrAccessDenied
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
		return ErrAccessDenied
	}
}

func (l *Listener) compareCacheSha2PasswordAuthData(c *Conn, clientAuthData []byte, salt []byte, password string) error {
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if password == "" {
			return nil
		}
		return ErrAccessDenied
	}
	exceptedData := CalcCachingSha2Password(salt, password)
	// the caching of 'caching_sha2_password' in MySQL, see: https://dev.mysql.com/worklog/task/?id=9591
	if bytes.Equal(exceptedData, clientAuthData) {
		// 'fast' auth: write "More data" packet (first byte == 0x01) with the second byte = 0x03
		return c.writeAuthMoreDataFastAuth()
	}
	return ErrAccessDenied

	//return c.fastShaCacheAuth(clientAuthData, handshakeInfo)
}

//https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html
func (l *Listener) cachingSha2AuthenticationExchange(c *Conn, clientAuthData []byte, handshakeInfo HandshakeResult) error {
	// other type of credential provider, we use the cache
	cached, ok := ShaPasswordCache.Load(fmt.Sprintf("%s@%s", handshakeInfo.User, c.LocalAddr()))
	if ok {
		// Scramble validation
		if compareScrambleSha2(cached.([]byte), handshakeInfo.Salt, clientAuthData) {
			// 'fast' auth: write "More data" packet (first byte == 0x01) with the second byte = 0x03
			return c.writeAuthMoreDataFastAuth()
		}
		return ErrAccessDenied
	}
	// cache miss, do full auth
	if err := c.writeAuthMoreDataFullAuth(); err != nil {
		return err
	}
	c.cachingSha2FullAuth = true
	return nil
}

func (l *Listener) handleClientHandshake(c *Conn, info *HandshakeResult, password string) error {

	if len(info.AuthResponse) == 0 && password == "" {
		return nil
	}

	if info.AuthPlugin != DefaultAuthPlugin && info.ClientPluginAuth {
		if err := c.writeAuthSwitchRequest(DefaultAuthPlugin, info.Salt); err != nil {
			return err
		}
		info.AuthPlugin = DefaultAuthPlugin
		err := l.readMoreAuthData(c, info)
		if err != nil {
			return err
		}
		return l.handleClientHandshake(c, info, password)
	}

	switch info.AuthPlugin {
	case MysqlNativePassword:
		if !bytes.Equal(CalcMySqlNativePassword(info.Salt, []byte(password)), info.AuthResponse) {
			return ErrAccessDenied
		}
		return nil

	case MysqlCachingSha2Password:
		if !c.cachingSha2FullAuth {
			// Switched auth method but no MoreData packet send yet
			if err := l.compareCacheSha2PasswordAuthData(c, info.AuthResponse, info.Salt, password); err != nil {
				return err
			} else {
				if c.cachingSha2FullAuth {
					err := l.readMoreAuthData(c, info)
					if err != nil {
						return err
					}
					return l.handleClientHandshake(c, info, password)
				}
				return nil
			}
		}
		// AuthMoreData packet already sent, do full auth
		if err := c.handleCachingSha2PasswordFullAuth(info.AuthResponse, info.Salt, password); err != nil {
			return err
		}
		c.writeCachingSha2Cache(info.User, password)
		return nil

	case MysqlSha256Password:
		err := l.handlePublicKeyRetrieval(c, info, info.AuthResponse, password)
		if err != nil {
			return err
		}
		return l.compareSha256PasswordAuthData(c, info.Salt, info.AuthResponse, password)

	default:
		return fmt.Errorf("unknown authentication plugin name '%s'", info.AuthPlugin)
	}
}

func (l *Listener) readMoreAuthData(c *Conn, info *HandshakeResult) error {
	authData, err := c.readAuthSwitchResponsePacket()
	info.AuthResponse = authData
	if err != nil {
		return err
	}
	return nil
}

// Public Key Retrieval
// See: https://dev.mysql.com/doc/internals/en/public-key-retrieval.html
func (l *Listener) handlePublicKeyRetrieval(c *Conn, info *HandshakeResult, authData []byte, password string) error {
	// if the client use 'sha256_password' auth method, and request for a public key
	// we send back a keyfile with Protocol::AuthMoreData
	if len(authData) == 1 && authData[0] == 0x01 {
		if c.Capabilities&CapabilityClientSSL == 0 {
			return errors.New("server does not support SSL: CLIENT_SSL not enabled")
		}
		cfg, ok := l.getTLSConfig()
		if !ok {
			return errors.New("server does not support SSL: CLIENT_SSL not enabled")
		}

		pubKey := cfg.Certificates[0].PrivateKey.(*rsa.PrivateKey).PublicKey
		keyBytes, err := x509.MarshalPKIXPublicKey(pubKey)
		if err != nil {
			return errors.New("marshal rsa public key fault.")
		}

		if err = c.writeAuthMoreDataPubKey(keyBytes); err != nil {
			return err
		}

		return l.handleClientHandshake(c, info, password)
	}
	return nil
}

func (c *Conn) handleCachingSha2PasswordFullAuth(authData []byte, salt []byte, password string) error {

	if len(authData) == 1 && authData[0] == 0x02 {
		// send the public key
		if err := c.writeAuthMoreDataFullAuth(); err != nil {
			return err
		}
		// read the encrypted password
		var err error
		if authData, err = c.readAuthSwitchResponsePacket(); err != nil {
			return err
		}
	}
	// the encrypted password
	// decrypt
	dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (tlsConfig.Certificates[0].PrivateKey).(*rsa.PrivateKey), authData, nil)
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
	return ErrAccessDenied
}

func (c *Conn) writeCachingSha2Cache(user string, password string) {
	// write cache
	if password == "" {
		return
	}
	m2 := generateScrambleData(password)
	// caching_sha2_password will maintain an in-memory hash of `user`@`host` => SHA256(SHA256(PASSWORD))

	ShaPasswordCache.Store(fmt.Sprintf("%s@%s", user, c.LocalAddr()), m2)
}

func generateScrambleData(password string) []byte {
	// SHA256(PASSWORD)
	crypt := sha256.New()
	crypt.Write([]byte(password))
	m1 := crypt.Sum(nil)
	// SHA256(SHA256(PASSWORD))
	crypt.Reset()
	crypt.Write(m1)
	m2 := crypt.Sum(nil)
	return m2
}
