package sc

type ProgrammaticCombinator struct {
	f RefFunc
}

type RefFunc func(Reference) (interface{}, error)

func NewProgrammatic(f RefFunc) ProgrammaticCombinator {
	return ProgrammaticCombinator{f: f}
}

func (c ProgrammaticCombinator) Get(r Reference) (interface{}, error) {
	return c.f(r)
}
func (c ProgrammaticCombinator) Put(Reference, interface{}) error {
	return unimplemented(c, "Put")
}
func (c ProgrammaticCombinator) Delete(Reference) error {
	return unimplemented(c, "Delete")
}
func (c ProgrammaticCombinator) Merge(Reference, interface{}) error {
	return unimplemented(c, "Merge")
}
