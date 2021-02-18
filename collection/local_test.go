package collection

import (
	"context"
	"testing"

	"github.com/przebro/databazaar/collection"
	local "github.com/przebro/localstore/internal/file"

	tst "github.com/przebro/databazaar/collection/testing"
)

var col collection.DataCollection

var (
	singleRecord       = tst.TestDocument{Title: "Blade Runner", Score: 8.1, Year: 1982, Oscars: false}
	singleRecordWithID tst.TestDocument
	testCollection     []tst.TestDocument
)

func init() {

	singleRecordWithID, testCollection = tst.GetSingleRecord("../data/testdata.json")
	manager := local.GetFileManager("../")

	data, err := manager.NewData("testcollection", 1, false)
	if err != nil {
		panic("")
	}

	col = Collection(data)
}

func TestInsertOne(t *testing.T) {

	r, err := col.Create(context.Background(), &singleRecord)

	if err == nil {
		t.Error(err)
	}

	if err != collection.ErrEmptyOrInvalidID {
		t.Error(err)
	}

	r, err = col.Create(context.Background(), &singleRecordWithID)
	if err != nil {
		t.Error(err)
	}

	if r.ID != singleRecordWithID.ID {
		t.Error("unexpected result, expected:", singleRecordWithID.ID, "actual:", r.ID)
	}

	x := func() {}
	r, err = col.Create(context.Background(), x)
	if err == nil {
		t.Error(err)
	}
}

func TestInsertMany(t *testing.T) {

	docs := []interface{}{}

	for n := range testCollection {
		docs = append(docs, testCollection[n])
	}
	r, err := col.CreateMany(context.Background(), docs)

	if err != nil {
		t.Error(err)
	}

	if len(r) != len(docs) {
		t.Error("unexpected result:", len(r))
	}

	docs = []interface{}{}
	docs = append(docs, singleRecord)

	_, err = col.CreateMany(context.Background(), docs)

	if err == nil {
		t.Error(err)
	}

	x := func() {}
	docs = append([]interface{}{}, x)

	_, err = col.CreateMany(context.Background(), docs)

	if err == nil {
		t.Error(err)
	}

}

func TestGetOne(t *testing.T) {

	doc := tst.TestDocument{}
	err := col.Get(context.Background(), "single_record", &doc)
	if err != nil {
		t.Error(err)
	}

	if doc.ID != singleRecordWithID.ID {
		t.Error("unexpected result:", doc.ID)
	}

	err = col.Get(context.Background(), "invalid_id", &doc)
	if err == nil {
		t.Error("unexpected result")
	}
}

func TestUpdate(t *testing.T) {

	singleRecord.Score = 7.3

	err := col.Update(context.Background(), &singleRecord)
	if err == nil {
		t.Error("unexpected result")
	}

	doc := tst.TestDocument{
		ID:     "movie_13",
		Oscars: true,
		Score:  7.9,
		Year:   1999,
		Title:  "The Matrix",
	}
	result, err := col.Create(context.Background(), &doc)
	if err != nil {
		t.Error("unexpected result:", err)
	}

	doc.ID = result.ID
	doc.REV = result.Revision
	doc.Score = 2.3
	err = col.Update(context.Background(), &doc)
	if err != nil {
		t.Error("unexpected result:", err)
	}

	x := func() {}

	err = col.Update(context.Background(), x)
	if err == nil {
		t.Error("unexpected result")
	}

}

func TestAsQuerable(t *testing.T) {

	_, err := col.AsQuerable()
	if err == nil {
		t.Error("unexpected result")
	}
}
func TestCount(t *testing.T) {
	s, _ := col.Count(context.Background())
	if s == 0 {
		t.Error("unexpected result:", s)
	}
}
func TestAll(t *testing.T) {
	crsr, err := col.All(context.Background())
	if err != nil {
		t.Error("unexpected result")
	}
	doc := &tst.TestDocument{}
	num := 0
	for crsr.Next(context.Background()) {
		crsr.Decode(&doc)
		num++
	}
	if num == 0 {
		t.Error("unexpected result:", num)
	}

	crsr.Close()
}

func TestDelete(t *testing.T) {

	err := col.Delete(context.Background(), singleRecordWithID.ID)

	if err != nil {
		t.Error("unexpected result:", err)
	}

	err = col.Delete(context.Background(), "")

	if err == nil {
		t.Error("unexpected result:", err)
	}
}
