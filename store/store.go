package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"

	local "github.com/przebro/localstore/collection"
	file "github.com/przebro/localstore/internal/file"

	"github.com/przebro/databazaar/collection"
	"github.com/przebro/databazaar/store"
	o "github.com/przebro/databazaar/store"
)

var localstore = "local"

const (
	optSyncTime   = "synctime"
	optUpdateSync = "updatesync"
)

type localStore struct {
	updsync  bool
	synctime int
	manager  file.FileManager
}

func init() {
	store.RegisterStoreFactory(localstore, initLocalstore)
}

func initLocalstore(opt o.ConnectionOptions) (store.DataStore, error) {

	var updsync bool
	var synctime int
	if opt.Path == "" {
		return nil, errors.New("invalid path")
	}

	dir, err := os.Stat(opt.Path)
	if err != nil || dir.IsDir() != true {
		return nil, errors.New("invalid path")
	}

	strtsync := opt.Options[optSyncTime]
	if strtsync != "" {

		if i, err := strconv.ParseInt(strtsync, 0, 32); err == nil {

			if i < 0 || i > 3600 {
				return nil, errors.New("invalid sync time value")
			}

			synctime = int(i)

		} else {

			return nil, errors.New("invalid sync time value")
		}
	}

	strusync := opt.Options[optUpdateSync]
	if strusync != "" {

		if v, err := strconv.ParseBool(strusync); err == nil {
			updsync = v
		} else {
			return nil, err
		}
	}
	m := file.GetFileManager(opt.Path)

	return &localStore{manager: m, updsync: updsync, synctime: synctime}, nil
}

//CreateCollection - Creates a new collection
func (s *localStore) CreateCollection(ctx context.Context, name string) (collection.DataCollection, error) {

	if ok, _ := regexp.Match(`^[A-Za-z][\d\w]{0,31}$`, []byte(name)); !ok {
		return nil, errors.New("invalid collection name")
	}

	fdata, err := s.manager.NewData(name, s.synctime, s.updsync)
	if err != nil {
		return nil, err
	}

	return local.Collection(fdata), nil

}

//Collection - gets a collection with a given name or returns an error if collection not found
func (s *localStore) Collection(ctx context.Context, name string) (collection.DataCollection, error) {

	fdata, err := s.manager.GetData(name, s.synctime, s.updsync)

	if err != nil {
		return nil, err
	}

	return local.Collection(fdata), nil

}

//Status - returns status of store
func (s *localStore) Status(context.Context) (string, error) {

	st, err := os.Stat(s.manager.Path())
	if err != nil {
		return "", err
	}

	data := fmt.Sprintf(`{"name" : "%s", "size" : %d, "modtime" : "%s"}`, st.Name(), st.Size(), st.ModTime().String())

	return data, nil

}

//Close - closes the store
func (s *localStore) Close(ctx context.Context) {
	s.manager.Close()
}
