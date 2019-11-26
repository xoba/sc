package sc

import "fmt"

func unimplemented(i interface{}, method string) error {
	return fmt.Errorf("%T.%s unimplemented", i, method)
}
