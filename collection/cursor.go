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

//NewCursor - creates a new cursor
func NewCursor(data []json.RawMessage) collection.BazaarCursor {
	crsr := &cursor{data: data, pos: -1}
	return crsr
}

//All - iterates over the result and appends a value to v
func (c *cursor) All(ctx context.Context, v interface{}) error {

	rval := reflect.ValueOf(v)
	if rval.Kind() != reflect.Ptr {
		return fmt.Errorf("not a pointer")
	}

	sval := rval.Elem()
	if sval.Kind() == reflect.Interface {
		sval = sval.Elem()
	}

	if sval.Kind() != reflect.Slice {
		return fmt.Errorf("not a slice")
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

//Next - gets next value from the cursor
func (c *cursor) Next(ctx context.Context) bool {

	c.pos++
	if c.pos >= len(c.data) {
		return false
	}

	return true
}

//Decode - decodes current value
func (c *cursor) Decode(v interface{}) error {

	return json.Unmarshal(c.data[c.pos], v)

}

//Close - closes the cursor
func (c *cursor) Close() error {

	return nil
}
