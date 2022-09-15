package utils

type AnyObj = interface{}
type AnyMap = map[string]AnyObj
type AnyList = []AnyObj

type Any interface {
	OK() bool
	Any() AnyObj
	Map() *Map
	List() *List
	String() (string, bool)
}

type GenericAny struct {
	o  AnyObj
	ok bool
}

type Map struct {
	GenericAny
	m AnyMap
}
type List struct {
	GenericAny
	l AnyList
}

var (
	NilAny  = &GenericAny{o: nil, ok: false}
	NilMap  = &Map{GenericAny: *NilAny, m: nil}
	NilList = &List{GenericAny: *NilAny, l: nil}
)

func AsAny(value AnyObj) Any {
	switch v := value.(type) {
	case Any:
		return v
	default:
		return &GenericAny{o: value, ok: true}
	}
}

func AsMap(value AnyMap) *Map {
	return &Map{GenericAny: GenericAny{o: value, ok: true}, m: value}
}

func AsList(value AnyList) *List {
	return &List{GenericAny: GenericAny{o: value, ok: true}, l: value}
}

func (a *GenericAny) OK() bool {
	return a != nil && a.o != nil && a.ok
}

func (a *GenericAny) Any() AnyObj {
	if a.OK() {
		return a.o
	}
	return nil
}

func (a *GenericAny) Map() *Map {
	if !a.OK() {
		return NilMap
	}
	if o, ok := a.o.(AnyMap); ok {
		return &Map{GenericAny: *a, m: o}
	}
	return NilMap
}

func (a *GenericAny) List() *List {
	if !a.OK() {
		return NilList
	}
	if o, ok := a.o.(AnyList); ok {
		return &List{GenericAny: *a, l: o}
	}
	return NilList
}

func (a *GenericAny) String() (string, bool) {
	if !a.OK() {
		return "", false
	}
	if o, ok := a.o.(string); ok {
		return o, true
	}
	return "", false
}

func (m *Map) OK() bool {
	return m.GenericAny.OK() && m.m != nil
}

func (m *Map) Get(key string) *GenericAny {
	if !m.OK() {
		return NilAny
	}
	if o, ok := m.m[key]; ok {
		return &GenericAny{o: o, ok: true}
	}
	return NilAny
}

func (m *Map) Set(key string, value AnyObj) Any {
	if !m.OK() {
		return NilAny
	}
	v := AsAny(value)
	if v.OK() {
		m.m[key] = v.Any()
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

func (l *List) Get(i int) *GenericAny {
	if i < 0 || i >= l.Len() {
		return NilAny
	}
	return &GenericAny{o: l.l[i], ok: true}
}

func (l *List) Append(value ...AnyObj) *List {
	if !l.OK() {
		return l
	}
	for _, v := range value {
		anyValue := AsAny(v)
		if anyValue.OK() {
			l.l = append(l.l, anyValue.Any())
			l.o = l.l
		}
	}
	return l
}
