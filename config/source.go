package config

import (
	"github.com/XiaoMi/Gaea/provider"
	"go.uber.org/config"
)

const (
	FileProvider = "file"
	EtcdProvider = "etcd"
)

// Configuration source provider
type Source interface {
	provider.Provider
	OnLoad(config config.Provider) error
	Close() error
}
