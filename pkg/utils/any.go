// Author: Liran Funaro <liran.funaro@ibm.com>

package utils

import (
	"reflect"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type AnyObj = interface{}
type AnyMap = map[string]AnyObj
type AnyList = []AnyObj

type Any interface {
	GetError() error
	OK() bool
	Obj() (AnyObj, error)
	Map() *Map
	List() *List
	String() (string, error)
	Marshal() ([]byte, error)
}

type GenericAny struct {
	o   AnyObj
	err error
}

type Map struct {
	GenericAny
	m AnyMap
}

type List struct {
	GenericAny
	l AnyList
}

func WrapError(err error) Any {
	return &GenericAny{o: nil, err: err}
}

func WrapAny(value AnyObj) Any {
	vAny, ok := value.(Any)
	if ok {
		return vAny
	} else {
		return &GenericAny{o: value}
	}
}

func AsMap(value AnyMap) *Map {
	return &Map{GenericAny: GenericAny{o: value}, m: value}
}

func AsList(value AnyList) *List {
	return &List{GenericAny: GenericAny{o: value}, l: value}
}

func (a *GenericAny) OK() bool {
	return a != nil && a.o != nil && a.err == nil
}

func (a *GenericAny) GetError() error {
	if a == nil {
		return errors.New("nil pointer")
	} else if a.err != nil {
		return a.err
	} else if a.o == nil {
		return errors.New("nil object")
	}
	return nil
}

func (a *GenericAny) Obj() (AnyObj, error) {
	return a.o, a.GetError()
}

func (a *GenericAny) Map() *Map {
	wasOK := a.OK()
	m, ok := a.o.(AnyMap)
	ret := &Map{GenericAny: *a, m: m}
	if !wasOK {
		return ret
	} else if !ok {
		ret.err = errors.Errorf("cannot interpret '%s' as map", reflect.TypeOf(a.o).String())
	}
	return ret
}

func (a *GenericAny) List() *List {
	wasOK := a.OK()
	l, ok := a.o.(AnyList)
	ret := &List{GenericAny: *a, l: l}
	if !wasOK {
		return ret
	} else if !ok {
		ret.err = errors.Errorf("cannot interpret '%s' as list", reflect.TypeOf(a.o).String())
	}
	return ret
}

func (a *GenericAny) String() (string, error) {
	if err := a.GetError(); err != nil {
		return "", err
	}
	if o, ok := a.o.(string); ok {
		return o, nil
	}
	return "", errors.Errorf("cannot interpret '%s' as string", reflect.TypeOf(a.o).String())
}

func (m *Map) OK() bool {
	return m.GenericAny.OK() && m.m != nil
}

func (m *Map) Get(key string) Any {
	if err := m.GetError(); err != nil {
		return WrapError(errors.Wrapf(err, "can't fetch key '%s'", key))
	}
	if o, ok := m.m[key]; ok {
		return &GenericAny{o: o}
	}
	return WrapError(errors.Errorf("no such key '%s'", key))
}

func (m *Map) Set(key string, value AnyObj) Any {
	if err := m.GetError(); err != nil {
		return WrapError(errors.Wrapf(err, "can't set key '%s'", key))
	}
	v := WrapAny(value)
	o, err := v.Obj()
	if err != nil {
		m.m[key] = o
	}
	return v
}

func (m *Map) SetDefault(key string, defaultVal AnyObj) Any {
	if s := m.Get(key); s.OK() {
		return s
	}
	return m.Set(key, defaultVal)
}

func (m *Map) SetDefaultList(key string) *List {
	if s := m.Get(key).List(); s.OK() {
		return s
	}
	return m.Set(key, AnyList{}).List()
}

func (l *List) OK() bool {
	return l.GenericAny.OK() && l.l != nil
}

func (l *List) Len() int {
	if !l.OK() {
		return 0
	}
	return len(l.l)
}

func (l *List) Get(i int) Any {
	if err := l.GetError(); err != nil {
		return WrapError(errors.Wrapf(err, "can't get index '%d'", i))
	}
	if i < 0 || i >= l.Len() {
		return WrapError(errors.Errorf("index '%d' out of bound", i))
	}
	return &GenericAny{o: l.l[i]}
}

func (l *List) Append(value ...AnyObj) *List {
	if !l.OK() {
		return l
	}
	for _, v := range value {
		if o, err := WrapAny(v).Obj(); err != nil {
			l.l = append(l.l, o)
			l.o = l.l
		}
	}
	return l
}

func Unmarshal(in []byte) Any {
	conf := &GenericAny{}
	conf.err = yaml.Unmarshal(in, &conf.o)
	return conf
}

func (a *GenericAny) Marshal() ([]byte, error) {
	o, err := a.Obj()
	if err != nil {
		return nil, err
	}
	return yaml.Marshal(o)
}
