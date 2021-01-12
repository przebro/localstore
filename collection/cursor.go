package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/przebro/databazaar/collection"
)

type cursor struct {
	data []json.RawMessage
	pos  int
}

func NewCursor(data []json.RawMessage) collection.BazaarCursor {
	crsr := &cursor{data: data, pos: -1}
	return crsr
}

func (c *cursor) All(ctx context.Context, v interface{}) error {

	rval := reflect.ValueOf(v)
	if rval.Kind() != reflect.Ptr {
		return fmt.Errorf("")
	}

	sval := rval.Elem()
	if sval.Kind() == reflect.Interface {
		sval = sval.Elem()
	}

	if sval.Kind() != reflect.Slice {
		return fmt.Errorf("")
	}

	etype := sval.Type().Elem()

	for x := c.pos; x < len(c.data); x++ {

		newElem := reflect.New(etype)
		i := newElem.Interface()
		json.Unmarshal(c.data[x], i)
		sval.Set(reflect.Append(sval, newElem.Elem()))
	}

	return nil
}
func (c *cursor) Next(ctx context.Context) bool {

	c.pos++
	if c.pos >= len(c.data) {
		return false
	}

	return true
}
func (c *cursor) Decode(v interface{}) error {

	return json.Unmarshal(c.data[c.pos], v)

}
func (c *cursor) Close() error {

	return nil
}
