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

package source

import (
	"errors"
	"github.com/XiaoMi/Gaea/config"
	"github.com/XiaoMi/Gaea/logging"
	"github.com/XiaoMi/Gaea/util"
	cnf "go.uber.org/config"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
)

// ErrClosedEtcdClient means etcd client closed
var ErrClosedEtcdClient = errors.New("use of closed etcd client")

var logger = logging.GetLogger("config")

const (
	defaultEtcdPath      = "/sharding-proxy"
	defaultEtcdEndpoints = "http://127.0.0.1:2379"
)

// etcdSource etcd client
type etcdSource struct {
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

func (c *etcdSource) GetName() string {
	return config.EtcdProvider
}

func NewEtcdSource() config.Source {
	return &etcdSource{}
}

func (c *etcdSource) OnLoad(provider cnf.Provider) error {
	etcdCnf := &etcdConfig{
		Endpoints: defaultEtcdEndpoints,
		Username:  "",
		Password:  "",
		Timeout:   time.Second * 10,
		Path:      defaultEtcdPath,
	}
	if err := provider.Get("etcd").Populate(etcdCnf); err == nil {
		etcdCnf.Endpoints = util.IfBlankAndTrim(etcdCnf.Endpoints, defaultEtcdEndpoints)
		etcdCnf.Path = util.IfBlankAndTrim(etcdCnf.Path, defaultEtcdPath)
	} else {
		logger.Warn("Parse etcd config fault.", util.LineSeparator, err)
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
		return err
	} else {
		c.kapi = client.NewKeysAPI(etcdClient)
		c.config = etcdCnf
	}
	return nil
}

// Close close etcd client
func (c *etcdSource) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return nil
}
