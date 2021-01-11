package provider

import (
	"errors"
	"fmt"
	"sync"
)

var onceReg sync.Once
var instance Registry

type Registry interface {
	TryLoad(tp Type, name string) (Provider, bool)
	Load(tp Type, name string) Provider
	Register(tp Type, provider Provider) error
	LoadOrStore(tp Type, name string, creation func() Provider) (actual Provider, loaded bool)
	LoadAndDelete(tp Type, name string) (value Provider, loaded bool)
	Delete(tp Type, name string)
}

func DefaultRegistry() Registry {
	onceReg.Do(func() {
		instance = &registry{sync.Map{}}
	})
	return instance
}

type registry struct {
	mp sync.Map
}

func getFullName(tp Type, name string) string {
	return fmt.Sprintf("%d:%s", int(tp), name)
}

func (r *registry) TryLoad(tp Type, name string) (Provider, bool) {
	fullName := getFullName(tp, name)
	v, ok := r.mp.Load(fullName)
	if ok {
		p, ok := v.(Provider)
		return p, ok
	}
	return nil, ok
}

func (r *registry) Load(tp Type, name string) Provider {
	fullName := getFullName(tp, name)
	v, ok := r.mp.Load(fullName)
	if !ok {
		panic(fmt.Errorf("provider named '%s' was not found", name))
	}
	return v.(Provider)
}

func (r *registry) Register(tp Type, provider Provider) error {
	if provider == nil {
		return errors.New("provider can not be null")
	}
	n := provider.GetName()
	fullName := getFullName(tp, n)
	if len(n) > 0 {
		r.mp.Store(fullName, provider)
		return nil
	} else {
		return errors.New("provider name can not be empty")
	}
}

func (r *registry) LoadOrStore(tp Type, name string, creation func() Provider) (actual Provider, loaded bool) {
	v, ok := r.TryLoad(tp, name)
	if !ok {
		v = creation()
		r.mp.Store(name, v)
	}
	return v, ok
}

func (r *registry) LoadAndDelete(tp Type, name string) (value Provider, loaded bool) {
	fullName := getFullName(tp, name)
	v, ok := r.mp.LoadAndDelete(fullName)
	if ok {
		p, ok := v.(Provider)
		return p, ok
	} else {
		return nil, ok
	}
}

func (r *registry) Delete(tp Type, name string) {
	fullName := getFullName(tp, name)
	r.mp.Delete(fullName)
}
