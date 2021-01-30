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

package backend

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/mysql"
)

//return data, switchplugin, err
func (dc *DirectConnection) readAuthResult() ([]byte, string, error) {
	data, err := dc.readPacket()
	if err != nil {
		return nil, "", err
	}

	// see: https://insidemysql.com/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	// packet indicator
	switch data[0] {

	case mysql.OKHeader:
		_, err := dc.handleOKPacket(data)
		return nil, "", err

	case mysql.MoreDataPacket:
		return data[1:], "", err

	case mysql.EOFHeader:
		// server wants to switch auth
		if len(data) < 1 {
			// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::OldAuthSwitchRequest
			return nil, mysql.AUTH_MYSQL_OLD_PASSWORD, nil
		}
		pluginEndIndex := bytes.IndexByte(data, 0x00)
		if pluginEndIndex < 0 {
			return nil, "", errors.New("invalid packet")
		}
		plugin := string(data[1:pluginEndIndex])
		authData := data[pluginEndIndex+1:]
		return authData, plugin, nil

	default: // Error otherwise
		return nil, "", dc.handleErrorPacket(data)
	}
}

// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchResponse
func (dc *DirectConnection) WriteAuthSwitchPacket(authData []byte, addNUL bool) error {
	pktLen := len(authData)
	if addNUL {
		pktLen++
	}
	data := make([]byte, pktLen)

	// Add the auth data [EOF]
	copy(data[:], authData)
	if addNUL {
		data[pktLen-1] = 0x00
	}

	err := dc.writePacket(data)

	return err
}

func (c *DirectConnection) WriteEncryptedPassword(password string, seed []byte, pub *rsa.PublicKey) error {
	enc, err := mysql.EncryptPassword(password, seed, pub)
	if err != nil {
		return err
	}
	return c.WriteAuthSwitchPacket(enc, false)
}

func (dc *DirectConnection) handleAuthResult() error {
	data, switchToPlugin, err := dc.readAuthResult()
	if err != nil {
		return err
	}
	// handle auth switch, only support 'sha256_password', and 'caching_sha2_password'
	if switchToPlugin != "" {
		//fmt.Printf("now switching auth plugin to '%s'\n", switchToPlugin)
		if data == nil {
			data = dc.salt
		} else {
			copy(dc.salt, data)
		}
		dc.authPluginName = switchToPlugin
		auth, err := dc.CalcPassword(data)
		if err = dc.WriteAuthSwitchPacket(auth, false); err != nil {
			return err
		}

		// Read Result Packet
		data, switchToPlugin, err = dc.readAuthResult()
		if err != nil {
			return err
		}

		// Do not allow to change the auth plugin more than once
		if switchToPlugin != "" {
			return fmt.Errorf("can not switch auth plugin more than once")
		}
	}

	// handle caching_sha2_password
	if dc.authPluginName == mysql.AUTH_CACHING_SHA2_PASSWORD {
		if data == nil {
			return nil // auth already succeeded
		}
		if data[0] == mysql.CacheSha2FastAuthSucceed {
			if err = dc.readOK(); err == nil {
				return nil // auth successful
			}
		} else if data[0] == mysql.CacheSha2FullAuthRequired {
			// need full authentication
			//if dc.tlsConfig != nil || dc.proto == "unix" {
			//	if err = c.WriteClearAuthPacket(c.password); err != nil {
			//		return err
			//	}
			//} else {
			//	if err = c.WritePublicKeyAuthPacket(c.password, c.salt); err != nil {
			//		return err
			//	}
			//}
			return dc.WritePublicKeyAuthPacket(dc.password, dc.salt)
		} else {
			return errors.New("invalid packet")
		}
	} else if dc.authPluginName == mysql.AUTH_SHA256_PASSWORD {
		if len(data) == 0 {
			return nil // auth already succeeded
		}
		block, _ := pem.Decode(data)
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return err
		}
		// send encrypted password
		err = dc.WriteEncryptedPassword(dc.password, dc.salt, pub.(*rsa.PublicKey))
		if err != nil {
			return err
		}
		err = dc.readOK()
		return err
	}
	return nil
}
