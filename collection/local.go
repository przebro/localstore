package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	local "github.com/przebro/localstore/internal/file"

	"github.com/przebro/databazaar/collection"
	"github.com/przebro/databazaar/result"
	"github.com/przebro/databazaar/selector"
)

// LocalCollection - implements databazaar Collection interface
type LocalCollection struct {
	jsonData *local.JsonFileData
}

type resultCollector struct {
	r []result.BazaarResult
}

// Collect - implementation of a KeyCollector
func (c *resultCollector) Collect(key string) {

	c.r = append(c.r, result.BazaarResult{ID: key})
}

// Collection - wraps a local collection and returns as DataCollection
func Collection(d *local.JsonFileData) collection.DataCollection {
	return &LocalCollection{jsonData: d}
}

// Create - creates a new record in the collection
func (col *LocalCollection) Create(ctx context.Context, document interface{}) (*result.BazaarResult, error) {

	id, _, err := collection.RequiredFields(document)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, collection.ErrEmptyOrInvalidID
	}

	doc, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}

	err = col.jsonData.Insert(id, doc)
	if err != nil {
		return nil, err
	}

	return &result.BazaarResult{ID: id}, nil
}

// Get - returns a single record with given id from the collection, if the key not exists returns an error
func (col *LocalCollection) Get(ctx context.Context, id string, result interface{}) error {

	data, exists := col.jsonData.Get(id)

	if !exists {
		return collection.ErrNoDocuments
	}

	return json.Unmarshal(data, result)
}

// Update - updates a single record in the collection
func (col *LocalCollection) Update(ctx context.Context, doc interface{}) error {

	id, _, err := collection.RequiredFields(doc)
	if err != nil {
		return err
	}
	if id == "" {
		return collection.ErrEmptyOrInvalidID
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	return col.jsonData.Update(id, data)

}

// Delete - deletes a record from the collection
func (col *LocalCollection) Delete(ctx context.Context, id string) error {

	if id == "" {
		return collection.ErrEmptyOrInvalidID
	}
	col.jsonData.Delete(id)

	return nil
}

// Count - returns a total number of elements in a collection
func (col *LocalCollection) Count(ctx context.Context) (int64, error) {

	return col.jsonData.Count(), nil
}

// CreateMany - bulk insert records into the collection
func (col *LocalCollection) CreateMany(ctx context.Context, docs []interface{}) ([]result.BazaarResult, error) {

	fn := func(doc interface{}) (key string, value []byte, err error) {

		if key, _, err = collection.RequiredFields(doc); err != nil {
			return
		}

		if key == "" {
			err = collection.ErrEmptyOrInvalidID
			return
		}

		if value, err = json.Marshal(doc); err != nil {
			return
		}

		return
	}

	res := &resultCollector{r: []result.BazaarResult{}}

	err := col.jsonData.ForEach(docs, res, fn)

	return res.r, err

}

// BulkUpdate - bulk update/inserts records into the collection
func (col *LocalCollection) BulkUpdate(ctx context.Context, docs []interface{}) error {

	var key string
	var err error
	var val []byte

	keys := []string{}
	items := []json.RawMessage{}

	for _, doc := range docs {
		if key, _, err = collection.RequiredFields(doc); err != nil {
			return err
		}

		if key == "" {
			return collection.ErrEmptyOrInvalidID
		}

		if val, err = json.Marshal(doc); err != nil {
			return err
		}

		keys = append(keys, key)
		items = append(items, val)
	}

	col.jsonData.Bulk(keys, items)

	return nil

}

// All - returns all available documents from the collection
func (col *LocalCollection) All(ctx context.Context) (collection.BazaarCursor, error) {
	data := col.jsonData.All()
	return NewCursor(data), nil
}
func (col *LocalCollection) Select(ctx context.Context, s selector.Expr, fld selector.Fields) (collection.BazaarCursor, error) {

	fn := func(item json.RawMessage) bool {

		r := map[string]interface{}{}

		if err := json.Unmarshal(item, &r); err != nil {
			return false
		}

		return apply(r, s)
	}

	data, err := col.jsonData.Over(fn)
	fmt.Println(len(data), "is error:", err)

	return NewCursor(data), err
}

// AsQuerable - Normally this method should return QuerableCollection that allows querying the collection, but this is a simple key-value store
func (col *LocalCollection) AsQuerable() (collection.QuerableCollection, error) {
	return col, nil
}

func apply(item map[string]interface{}, s selector.Expr) bool {

	if sel, ok := s.(*selector.CmpExpr); ok {

		val, exists := item[sel.Field]
		if exists {
			if isValueExpr(sel.Ex) {
				return compare(sel.Op, val, sel.Ex)
			}

		}

		return false
	}
	if sel, ok := s.(*selector.LogExpr); ok {

		var result bool

		if sel.Op == selector.AndOperator {
			result = true
			for _, ex := range sel.Ex {
				result = result && apply(item, ex)
			}
		}
		if sel.Op == selector.OrOperator {
			result = false
			for _, ex := range sel.Ex {
				result = result || apply(item, ex)
			}
		}

		return result
	}

	return false

}

func compare(op string, val interface{}, expr selector.Expr) bool {

	if sel, ok := expr.(selector.Bool); ok {

		a := val.(bool)
		b := bool(sel)

		if op == selector.EqOperator {
			return a == b
		}
		if op == selector.NeOperator {
			return a != b
		}

	}

	if sel, ok := expr.(selector.Int); ok {
		a := int(val.(float64))
		b := int(sel)

		return evalNum(a, b, op)
	}
	if sel, ok := expr.(selector.Float); ok {
		a := val.(float64)
		b := float64(sel)

		return evalNum(a, b, op)
	}
	if sel, ok := expr.(selector.String); ok {
		a := val.(string)
		b := string(sel)

		r := strings.Compare(a, b)
		if op == selector.EqOperator && r == 0 {
			return true
		}

		if op == selector.NeOperator && r != 0 {
			return true
		}

		return false

	}

	return false
}

func evalNum[T int | float32 | float64](a, b T, op string) bool {

	if op == selector.EqOperator {
		return a == b
	}

	if op == selector.NeOperator {
		return a != b
	}
	if op == selector.GtOperator {
		return a > b
	}
	if op == selector.GteOperator {
		return a >= b
	}
	if op == selector.LtOperator {
		return a < b
	}
	if op == selector.LteOperator {
		return a <= b
	}

	return false
}

func isValueExpr(expr selector.Expr) bool {

	_, x := expr.(selector.CmpExpr)
	_, y := expr.(selector.LogExpr)

	if x || y {
		return false
	}

	return true
}
