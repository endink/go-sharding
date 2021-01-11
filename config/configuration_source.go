package config

import (
	"github.com/XiaoMi/Gaea/provider"
	"time"
)

// Configuration source provider
type SourceProvider interface {
	provider.Provider
	OnLoad()
	Create(path string, data []byte) error
	Update(path string, data []byte) error
	UpdateWithTTL(path string, data []byte, ttl time.Duration) error
	Delete(path string) error
	Read(path string) ([]byte, error)
	List(path string) ([]string, error)
	Close() error
	BasePrefix() string
}
