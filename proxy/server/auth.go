package server

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/mysql"
)

var ErrAccessDenied = errors.New("access denied")
var tlsConfig tls.Config

func (c *Session) auth(authInfo HandshakeResponseInfo, password string) error {
	clientAuthData := authInfo.AuthResponse
	switch authInfo.AuthPlugin {
	case mysql.AUTH_NATIVE_PASSWORD:
		return c.compareNativePasswordAuthData(clientAuthData, password)

	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		if err := c.compareCacheSha2PasswordAuthData(clientAuthData, password); err != nil {
			return err
		}
		if c.cachingSha2FullAuth {
			return c.handleAuthSwitchResponse(authInfo, password)
		}
		return nil

	case mysql.AUTH_SHA256_PASSWORD:
		//cont, err := c.handlePublicKeyRetrieval(clientAuthData)
		//if err != nil {
		//	return err
		//}
		//if !cont {
		//	return nil
		//}
		return c.compareSha256PasswordAuthData(clientAuthData, password)

	default:
		return fmt.Errorf("unknown authentication plugin name '%s'", authInfo.AuthPlugin)
	}
}

func scrambleValidation(cached, nonce, scramble []byte) bool {
	// SHA256(SHA256(SHA256(STORED_PASSWORD)), NONCE)
	crypt := sha256.New()
	crypt.Write(cached)
	crypt.Write(nonce)
	message2 := crypt.Sum(nil)
	// SHA256(PASSWORD)
	if len(message2) != len(scramble) {
		return false
	}
	for i := range message2 {
		message2[i] ^= scramble[i]
	}
	// SHA256(SHA256(PASSWORD)
	crypt.Reset()
	crypt.Write(message2)
	m := crypt.Sum(nil)
	return bytes.Equal(m, cached)
}

func (c *Session) compareNativePasswordAuthData(clientAuthData []byte, password string) error {
	if bytes.Equal(mysql.CalcPassword(c.c.salt, []byte(password)), clientAuthData) {
		return nil
	}
	return ErrAccessDenied
}

func (c *Session) compareSha256PasswordAuthData(clientAuthData []byte, password string) error {
	/*
		// Empty passwords are not hashed, but sent as empty string
		if len(clientAuthData) == 0 {
			if password == "" {
				return nil
			}
			return ErrAccessDenied
		}
		//tlsConn, isTls := c.Conn.(*tls.Conn);
		isTls := false
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
			dbytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, (c.serverConf.tlsConfig.Certificates[0].PrivateKey).(*rsa.PrivateKey), clientAuthData, nil)
			if err != nil {
				return err
			}
			plain := make([]byte, len(password)+1)
			copy(plain, password)
			for i := range plain {
				j := i % len(c.salt)
				plain[i] ^= c.salt[j]
			}
			if bytes.Equal(plain, dbytes) {
				return nil
			}
			return ErrAccessDenied
		}
	*/
	return fmt.Errorf("Sha256Password unsupported")
}

func (c *Session) compareCacheSha2PasswordAuthData(clientAuthData []byte, password string) error {
	// Empty passwords are not hashed, but sent as empty string
	if len(clientAuthData) == 0 {
		if password == "" {
			return nil
		}
		return ErrAccessDenied
	}
	// the caching of 'caching_sha2_password' in MySQL, see: https://dev.mysql.com/worklog/task/?id=9591
	if bytes.Equal(mysql.CalcCachingSha2Password(c.c.salt, password), clientAuthData) {
		// 'fast' auth: write "More data" packet (first byte == 0x01) with the second byte = 0x03
		return c.c.WriteAuthMoreDataFastAuth()
	}
	return ErrAccessDenied

	// other type of credential provider, we use the cache
	//cached, ok := c.serverConf.cacheShaPassword.Load(fmt.Sprintf("%s@%s", c.user, c.Conn.LocalAddr()))
	//if ok {
	//	// Scramble validation
	//	if scrambleValidation(cached.([]byte), c.salt, clientAuthData) {
	//		// 'fast' auth: write "More data" packet (first byte == 0x01) with the second byte = 0x03
	//		return c.writeAuthMoreDataFastAuth()
	//	}
	//	return ErrAccessDenied
	//}
	//// cache miss, do full auth
	//if err := c.writeAuthMoreDataFullAuth(); err != nil {
	//	return err
	//}
	//c.cachingSha2FullAuth = true
	//return nil
}

/************resonse handler***********/

func (c *Session) readAuthSwitchRequestResponse() ([]byte, error) {
	data, err := c.c.ReadPacket()
	if err != nil {
		return nil, err
	}
	if len(data) == 1 && data[0] == 0x00 {
		// \NUL
		return make([]byte, 0), nil
	}
	return data, nil
}

func (c *Session) handleAuthSwitchResponse(info HandshakeResponseInfo, password string) error {
	authData, err := c.readAuthSwitchRequestResponse()
	if err != nil {
		return err
	}

	switch info.AuthPlugin {
	case mysql.AUTH_NATIVE_PASSWORD:
		if !bytes.Equal(mysql.CalcPassword(c.c.salt, []byte(password)), authData) {
			return ErrAccessDenied
		}
		return nil

	case mysql.AUTH_CACHING_SHA2_PASSWORD:
		if !c.cachingSha2FullAuth {
			// Switched auth method but no MoreData packet send yet
			if err := c.compareCacheSha2PasswordAuthData(authData, password); err != nil {
				return err
			} else {
				if c.cachingSha2FullAuth {
					return c.handleAuthSwitchResponse(info, password)
				}
				return nil
			}
		}
		// AuthMoreData packet already sent, do full auth
		if err := c.handleCachingSha2PasswordFullAuth(authData, password); err != nil {
			return err
		}
		c.writeCachingSha2Cache(password)
		return nil

	case mysql.AUTH_SHA256_PASSWORD:
		//cont, err := c.handlePublicKeyRetrieval(authData)
		//if err != nil {
		//	return err
		//}
		//if !cont {
		//	return nil
		//}
		//if err := c.acquirePassword(); err != nil {
		//	return err
		//}
		return c.compareSha256PasswordAuthData(authData, password)

	default:
		return fmt.Errorf("unknown authentication plugin name '%s'", info.AuthPlugin)
	}
}

func (c *Session) handleCachingSha2PasswordFullAuth(authData []byte, password string) error {

	if len(authData) == 1 && authData[0] == 0x02 {
		// send the public key
		if err := c.c.writeAuthMoreDataFullAuth(); err != nil {
			return err
		}
		// read the encrypted password
		var err error
		if authData, err = c.readAuthSwitchRequestResponse(); err != nil {
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
		j := i % len(c.c.salt)
		plain[i] ^= c.c.salt[j]
	}
	if bytes.Equal(plain, dbytes) {
		return nil
	}
	return ErrAccessDenied
}

func (c *Session) writeCachingSha2Cache(password string) {
	// write cache
	if password == "" {
		return
	}
	// SHA256(PASSWORD)
	crypt := sha256.New()
	crypt.Write([]byte(password))
	m1 := crypt.Sum(nil)
	// SHA256(SHA256(PASSWORD))
	crypt.Reset()
	crypt.Write(m1)
	_ = crypt.Sum(nil)
	// caching_sha2_password will maintain an in-memory hash of `user`@`host` => SHA256(SHA256(PASSWORD))

	//c.serverConf.cacheShaPassword.Store(fmt.Sprintf("%s@%s", c.user, c.Conn.LocalAddr()), m2)
}
