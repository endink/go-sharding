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

package models

import (
	"strings"

	"github.com/go-ini/ini"
)

const (
	defaultGaeaCluster = "gaea"
	ConfigFile         = "file"
)

// Proxy means proxy structure of proxy source
type Proxy struct {
	// source type
	ConfigType string `yaml:"source-type"`

	// 文件配置类型内容
	FileConfigPath string `ini:"file-source-path"`

	// etcd 相关配置
	CoordinatorAddr string `yaml:"coordinator-addr"`
	CoordinatorRoot string `yaml:"coordinator-root"`
	UserName        string `yaml:"username"`
	Password        string `yaml:"password"`

	// 服务相关信息
	Environ string `yaml:"environ"`
	Service string `yaml:"service-name"`
	Cluster string `yaml:"cluster-name"`

	ProtoType      string `yaml:"proto-type"`
	ProxyAddr      string `yaml:"proxy-addr"`
	AdminAddr      string `yaml:"admin-addr"`
	AdminUser      string `yaml:"admin-user"`
	AdminPassword  string `yaml:"admin-password"`
	SlowSQLTime    int64  `yaml:"slow-sql_time"`
	SessionTimeout int    `yaml:"session-timeout"`

	// 监控配置
	StatsEnabled  string `yaml:"stats-enabled"`  // set true to enable stats
	StatsInterval int    `yaml:"stats-interval"` // set stats interval of connect pool

	EncryptKey string `ini:"encrypt-key"`
}

func DefaultProxy() *Proxy {
	return &Proxy{
		ConfigType:      ConfigFile,
		FileConfigPath:  ".",
		CoordinatorAddr: "http://127.0.0.1:2379",
		UserName:        "",
		Password:        "",
		Environ:         "online",
		Service:         "sharding_proxy",
		Cluster:         "sp_cluster",
		AdminAddr:       "0.0.0.0:13307",
		AdminUser:       "admin",
		AdminPassword:   "admin",
		ProtoType:       "tcp4",
		ProxyAddr:       "0.0.0.0:13306",
		SlowSQLTime:     1000,
		SessionTimeout:  3600,
		StatsEnabled:    "false",
		StatsInterval:   10,
		EncryptKey:      "00000000000000000",
	}
}

// ParseProxyConfigFromFile parser proxy source from file
func ParseProxyConfigFromFile(cfgFile string) (*Proxy, error) {
	cfg, err := ini.Load(cfgFile)

	if err != nil {
		return nil, err
	}

	var proxyConfig = &Proxy{}
	err = cfg.MapTo(proxyConfig)
	// default source type: etcd
	if proxyConfig.ConfigType == "" {
		proxyConfig.ConfigType = ConfigFile
	}
	if proxyConfig.Cluster == "" && proxyConfig.CoordinatorRoot == "" {
		proxyConfig.Cluster = defaultGaeaCluster
	} else if proxyConfig.Cluster == "" && proxyConfig.CoordinatorRoot != "" {
		proxyConfig.Cluster = strings.TrimPrefix(proxyConfig.CoordinatorRoot, "/")
	} else if proxyConfig.Cluster != "" {
		proxyConfig.CoordinatorRoot = "/" + proxyConfig.Cluster
	}
	return proxyConfig, err
}

// Verify verify proxy source
func (p *Proxy) Verify() error {
	return nil
}

// ProxyInfo for report proxy information
type ProxyInfo struct {
	Token     string `json:"token"`
	StartTime string `json:"start_time"`

	IP        string `json:"ip"`
	ProtoType string `json:"proto_type"`
	ProxyPort string `json:"proxy_port"`
	AdminPort string `json:"admin_port"`

	Pid int    `json:"pid"`
	Pwd string `json:"pwd"`
	Sys string `json:"sys"`
}

// Encode encode proxy info
func (p *ProxyInfo) Encode() []byte {
	return JSONEncode(p)
}
