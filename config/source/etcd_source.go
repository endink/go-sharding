/*
 *
 *  * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  File author: Anders Xiao
 *
 */

package source

import (
	"errors"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/logging"
	cnf "go.uber.org/config"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
)

const EtcdConfigProvider = "etcd"

var etcdLogger = logging.GetLogger("config-etcd")

// ErrClosedEtcdClient means etcd client closed
var ErrClosedEtcdClient = errors.New("use of closed etcd client")

const (
	defaultEtcdPath      = "/sharding-proxy"
	defaultEtcdEndpoints = "http://127.0.0.1:2379"
)

// etcdSource etcd client
type EtcdSource struct {
	sync.Mutex
	kapi   client.KeysAPI
	closed bool
	config *etcdConfig
}

type etcdConfig struct {
	Endpoints string
	Username  string
	Password  string
	Timeout   time.Duration
	Path      string
}

func (c *EtcdSource) GetName() string {
	return EtcdConfigProvider
}

func (c *EtcdSource) Load(provider cnf.Provider) (cnf.Value, error) {
	etcdCnf := &etcdConfig{
		Endpoints: defaultEtcdEndpoints,
		Username:  "",
		Password:  "",
		Timeout:   time.Second * 10,
		Path:      defaultEtcdPath,
	}
	if err := provider.Get("etcd").Populate(etcdCnf); err == nil {
		etcdCnf.Endpoints = core.IfBlankAndTrim(etcdCnf.Endpoints, defaultEtcdEndpoints)
		etcdCnf.Path = core.IfBlankAndTrim(etcdCnf.Path, defaultEtcdPath)
	} else {
		etcdLogger.Warn("Parse etcd config fault.", core.LineSeparator, err)
	}

	endpoints := strings.Split(etcdCnf.Endpoints, ",")
	for i, endpoint := range endpoints {
		url := strings.TrimSpace(endpoint)
		if !strings.HasPrefix(url, "http://") {
			url = "http://" + url
		}
		endpoints[i] = url
	}

	clientCnf := client.Config{
		Endpoints:               endpoints,
		Transport:               client.DefaultTransport,
		Username:                etcdCnf.Username,
		Password:                etcdCnf.Password,
		HeaderTimeoutPerRequest: time.Second * 10,
	}

	if etcdClient, err := client.New(clientCnf); err != nil {
		return cnf.Value{}, err
	} else {
		c.kapi = client.NewKeysAPI(etcdClient)
		c.config = etcdCnf
	}
	return cnf.Value{}, nil
}

// Close close etcd client
func (c *EtcdSource) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return nil
}
