package sc

func NewMemory() *Memory {
	return &Memory{m: make(map[string]interface{})}
}

type Memory struct {
	m map[string]interface{}
}

func (mem Memory) Get(r Reference) (interface{}, error) {
	i, ok := mem.m[key(r)]
	if !ok {
		return nil, NotFound
	}
	return i, nil
}

func key(r Reference) string {
	return r.URI().String()
}

func (mem Memory) Put(r Reference, i interface{}) error {
	mem.m[key(r)] = i
	return nil
}

func (mem Memory) Merge(r Reference, i interface{}) error {
	return unimplemented(mem, "Merge")
}

func (mem Memory) Delete(r Reference) error {
	if _, ok := mem.m[key(r)]; !ok {
		return NotFound
	}
	delete(mem.m, key(r))
	return nil
}
