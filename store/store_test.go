package store

import (
	"context"
	"os"
	"testing"

	"github.com/przebro/databazaar/store"
)

func TestCreateStore(t *testing.T) {

	ds, _ := store.NewStore("local;/../")
	_, err := ds.CreateCollection(context.Background(), "test")
	if err != nil {
		t.Error("unexpected result")
	}
}
func TestCollection(t *testing.T) {

	os.Mkdir("../local", 644)
	ds, _ := store.NewStore("local;/../local")
	_, err := ds.CreateCollection(context.Background(), "test")
	if err != nil {
		t.Error("unexpected result")
	}
	_, err = ds.CreateCollection(context.Background(), "test")
	if err == nil {
		t.Error("unexpected result")
	}

	_, err = ds.Collection(context.Background(), "test")
	if err != nil {
		t.Error("unexpected result")
	}

	_, err = ds.Collection(context.Background(), "test1")
	if err == nil {
		t.Error("unexpected result")
	}

	_, err = ds.CreateCollection(context.Background(), "*test")
	if err == nil {
		t.Error("unexpected result")
	}

	_, err = ds.CreateCollection(context.Background(), "Atest1234567890123456789012345677")
	if err == nil {
		t.Error("unexpected result")
	}

	os.Remove("../local")
}
func TestOptions(t *testing.T) {

	_, err := store.NewStore("local;/?synctime=12")

	if err == nil {
		t.Error("unexpected result")
	}

	_, err = store.NewStore("local;/../aBC?synctime=12")

	if err == nil {
		t.Error("unexpected result")
	}

	_, err = store.NewStore("local;/../?synctime=-1")

	if err == nil {
		t.Error("unexpected result")
	}

	_, err = store.NewStore("local;/../?synctime=3601")

	if err == nil {
		t.Error("unexpected result")
	}

	_, err = store.NewStore("local;/../?synctime=ABC")

	if err == nil {
		t.Error("unexpected result")
	}

	_, err = store.NewStore("local;/../?updatesync=aaa")

	if err == nil {
		t.Error("unexpected result")
	}
}

func TestStatus(t *testing.T) {

	ds, _ := store.NewStore("local;/../data")
	s, err := ds.Status(context.Background())
	if err != nil {
		t.Error(err)
	}
	if s == "" {
		t.Error("unexpected result")
	}

	ds.Close(context.Background())

}
