package collection

import (
	"context"
	"encoding/json"
	"errors"

	local "github.com/przebro/localstore/internal/file"

	"github.com/przebro/databazaar/collection"
	"github.com/przebro/databazaar/result"
)

//LocalCollection - implements databazaar Collection interface
type LocalCollection struct {
	jsonData *local.JsonFileData
}

type resultCollector struct {
	r []result.BazaarResult
}

//Collect - implementation of a KeyCollector
func (c *resultCollector) Collect(key string) {

	c.r = append(c.r, result.BazaarResult{ID: key})
}

//Collection - wraps a local collection and returns as DataCollection
func Collection(d *local.JsonFileData) collection.DataCollection {
	return &LocalCollection{jsonData: d}
}

//Create - creates a new record in a collection
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

//Get - returns a single record with given id from the collection,if the key not exists returns error
func (col *LocalCollection) Get(ctx context.Context, id string, result interface{}) error {

	data, exists := col.jsonData.Get(id)

	if !exists {
		return errors.New("keys does not exists")
	}

	return json.Unmarshal(data, result)
}

//Update - updates a single record in a collection
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

//Delete - deletes a record from a collection
func (col *LocalCollection) Delete(ctx context.Context, id string) error {

	if id == "" {
		return collection.ErrEmptyOrInvalidID
	}
	col.jsonData.Delete(id)

	return nil

}

//CreateMany - Bulk insert records into the collection
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

//AsQuerable - Normally this method should return QuerableCollection that allows querying the collection, but this is a simple key-value store
func (col *LocalCollection) AsQuerable() (collection.QuerableCollection, error) {
	return nil, errors.New("collection doesn't support query")
}
