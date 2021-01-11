// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

// Copyright 2019 The Gaea Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"context"
	"errors"
	"github.com/XiaoMi/Gaea/config"
	"github.com/XiaoMi/Gaea/logging"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
)

// ErrClosedEtcdClient means etcd client closed
var ErrClosedEtcdClient = errors.New("use of closed etcd client")

const (
	defaultEtcdPrefix = "/gaea"
)

// etcdSource etcd client
type etcdSource struct {
	sync.Mutex
	kapi client.KeysAPI

	closed  bool
	timeout time.Duration
	Prefix  string
}

func (c *etcdSource) GetName() string {
	panic("implement me")
}

func (c *etcdSource) OnLoad() {
	panic("implement me")
}

// New constructor of etcdSource
func NewEtcdSource(addr string, timeout time.Duration, username, passwd, root string) (config.SourceProvider, error) {
	endpoints := strings.Split(addr, ",")
	for i, s := range endpoints {
		if s != "" && !strings.HasPrefix(s, "http://") {
			endpoints[i] = "http://" + s
		}
	}
	config := client.Config{
		Endpoints:               endpoints,
		Transport:               client.DefaultTransport,
		Username:                username,
		Password:                passwd,
		HeaderTimeoutPerRequest: time.Second * 10,
	}
	c, err := client.New(config)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(root) == "" {
		root = defaultEtcdPrefix
	}
	return &etcdSource{
		kapi:    client.NewKeysAPI(c),
		timeout: timeout,
		Prefix:  root,
	}, nil
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

func (c *etcdSource) contextWithTimeout() (context.Context, context.CancelFunc) {
	if c.timeout == 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), c.timeout)
}

func isErrNoNode(err error) bool {
	if err != nil {
		if e, ok := err.(client.Error); ok {
			return e.Code == client.ErrorCodeKeyNotFound
		}
	}
	return false
}

func isErrNodeExists(err error) bool {
	if err != nil {
		if e, ok := err.(client.Error); ok {
			return e.Code == client.ErrorCodeNodeExist
		}
	}
	return false
}

// Mkdir create directory
func (c *etcdSource) Mkdir(dir string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	return c.mkdir(dir)
}

func (c *etcdSource) mkdir(dir string) error {
	if dir == "" || dir == "/" {
		return nil
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	_, err := c.kapi.Set(cntx, dir, "", &client.SetOptions{Dir: true, PrevExist: client.PrevNoExist})
	if err != nil {
		if isErrNodeExists(err) {
			return nil
		}
		return err
	}
	return nil
}

// Create create path with data
func (c *etcdSource) Create(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	logging.DefaultLogger.Debugf("etcd create node %s", path)
	_, err := c.kapi.Set(cntx, path, string(data), &client.SetOptions{PrevExist: client.PrevNoExist})
	if err != nil {
		logging.DefaultLogger.Debugf("etcd create node %s failed: %s", path, err)
		return err
	}
	logging.DefaultLogger.Debugf("etcd create node OK")
	return nil
}

// Update update path with data
func (c *etcdSource) Update(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	logging.DefaultLogger.Debugf("etcd update node %s", path)
	_, err := c.kapi.Set(cntx, path, string(data), &client.SetOptions{PrevExist: client.PrevIgnore})
	if err != nil {
		logging.DefaultLogger.Debugf("etcd update node %s failed: %s", path, err)
		return err
	}
	logging.DefaultLogger.Debugf("etcd update node OK")
	return nil
}

// UpdateWithTTL update path with data and ttl
func (c *etcdSource) UpdateWithTTL(path string, data []byte, ttl time.Duration) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	logging.DefaultLogger.Debugf("etcd update node %s with ttl %d", path, ttl)
	_, err := c.kapi.Set(cntx, path, string(data), &client.SetOptions{PrevExist: client.PrevIgnore, TTL: ttl})
	if err != nil {
		logging.DefaultLogger.Debugf("etcd update node %s failed: %s", path, err)
		return err
	}
	logging.DefaultLogger.Debugf("etcd update node OK")
	return nil
}

// Delete delete path
func (c *etcdSource) Delete(path string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	logging.DefaultLogger.Debugf("etcd delete node %s", path)
	_, err := c.kapi.Delete(cntx, path, nil)
	if err != nil && !isErrNoNode(err) {
		logging.DefaultLogger.Debugf("etcd delete node %s failed: %s", path, err)
		return err
	}
	logging.DefaultLogger.Debugf("etcd delete node OK")
	return nil
}

// Read read path data
func (c *etcdSource) Read(path string) ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	logging.DefaultLogger.Debugf("etcd read node %s", path)
	r, err := c.kapi.Get(cntx, path, nil)
	if err != nil && !isErrNoNode(err) {
		return nil, err
	} else if r == nil || r.Node.Dir {
		return nil, nil
	} else {
		return []byte(r.Node.Value), nil
	}
}

// List list path, return slice of all paths
func (c *etcdSource) List(path string) ([]string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	logging.DefaultLogger.Debugf("etcd list node %s", path)
	r, err := c.kapi.Get(cntx, path, nil)
	if err != nil && !isErrNoNode(err) {
		return nil, err
	} else if r == nil || !r.Node.Dir {
		return nil, nil
	} else {
		var files []string
		for _, node := range r.Node.Nodes {
			files = append(files, node.Key)
		}
		return files, nil
	}
}

// Watch watch path
func (c *etcdSource) Watch(path string, ch chan string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		panic(ErrClosedEtcdClient)
	}
	watcher := c.kapi.Watcher(path, &client.WatcherOptions{Recursive: true})
	for {
		res, err := watcher.Next(context.Background())
		if err != nil {
			panic(err)
		}
		ch <- res.Action
	}
}

// BasePrefix return base prefix
func (c *etcdSource) BasePrefix() string {
	return c.Prefix
}
