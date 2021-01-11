package provider

import "time"

// ConfigProvider client interface
type ConfigProvider interface {
	Provider
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
