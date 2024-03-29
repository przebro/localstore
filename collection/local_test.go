package collection

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/przebro/databazaar/collection"
	"github.com/przebro/databazaar/selector"
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

	data, err := manager.NewData("testcollection", 0, false)
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

func TestSelect(t *testing.T) {

	type testTable struct {
		ex       selector.Expr
		expected int
	}

	table := []testTable{
		{selector.Eq("oscars", selector.Bool(true)), 4},
		{selector.Eq("oscars", selector.Bool(false)), 5},
		{selector.Ne("oscars", selector.Bool(false)), 4},
		{selector.Eq("year", selector.Int(1986)), 2},
		{selector.Ne("year", selector.Int(1980)), 7},
		{selector.Gt("year", selector.Int(1980)), 6},
		{selector.Gte("year", selector.Int(1980)), 8},
		{selector.Lt("year", selector.Int(1980)), 1},
		{selector.Lte("year", selector.Int(1980)), 3},
	}

	prepareCollection()

	querable, _ := col.AsQuerable()

	for _, n := range table {
		crsr, _ := querable.Select(context.Background(), n.ex, selector.Fields{})
		c := crsr.(*cursor)
		if len(c.data) != n.expected {
			t.Error("unexpected result:", len(c.data))
		}
	}

	// //querable.Select(context.Background(), selector.Eq("oscars", selector.Bool(false)), selector.Fields{})
	// crsr, _ := querable.Select(context.Background(), selector.Gt("year", selector.Int(1985)), selector.Fields{})
	// c := crsr.(*cursor)
	// fmt.Println(len(c.data))

	// querable.Select(context.Background(), selector.Or(
	// 	selector.Eq("year", selector.Int(1982)),
	// 	selector.Eq("year", selector.Int(1998)),
	// ), []string{})
	// //year = 1984
	// // { $or :
}

func prepareCollection() {
	data, err := os.ReadFile("../data/testdata.json")
	if err != nil {
		panic("panic")
	}

	out := map[string]interface{}{}
	json.Unmarshal(data, &out)

	cc := out["collection"].([]interface{})
	for _, x := range cc {

		nn := x.(map[string]interface{})

		flt := nn["score"].(float64)
		yr := nn["year"].(float64)
		osc := nn["oscars"].(bool)

		doc := tst.TestDocument{
			ID:     nn["_id"].(string),
			Title:  nn["title"].(string),
			Score:  float32(flt),
			Year:   int(yr),
			Genre:  nn["genre"].(string),
			Oscars: osc,
		}

		col.Create(context.TODO(), doc)
	}

}
