package provider

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var onceReg sync.Once
var instance Registry

type Registry interface {
	TryLoad(tp Type, name string) (Provider, bool)
	Load(tp Type, name string) Provider
	TryLoadFirst(tp Type) (Provider, bool)
	LoadFirst(tp Type) Provider
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
	n := strings.TrimSpace(name)
	if n == "" {
		panic(errors.New("provider name can not be null"))
	}
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
	p, _ := r.TryLoad(tp, name)
	return p
}

func (r *registry) LoadFirst(tp Type) Provider {
	p, _ := r.TryLoadFirst(tp)
	return p
}

func (r *registry) TryLoadFirst(tp Type) (Provider, bool) {
	var ok bool
	var p Provider
	r.mp.Range(func(key, value interface{}) bool {
		keyStr := key.(string)
		if strings.HasPrefix(keyStr, fmt.Sprint(int(tp), ":")) {
			pro, isProvider := value.(Provider)
			if isProvider {
				ok = true
				p = pro
				return false
			}
		}
		return true
	})
	return p, ok
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
	fullName := getFullName(tp, name)
	v, ok := r.TryLoad(tp, fullName)
	if !ok {
		v = creation()
		r.mp.Store(fullName, v)
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
