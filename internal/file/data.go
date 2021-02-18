package localstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	errKeyNotFound         = errors.New("key not found")
	errKeyExists           = errors.New("key aleready exists")
	errCollectionNotExists = errors.New("collection does not exists")
	errCollectionExists    = errors.New("collection already exists")
)

//JsonFileData - inmemory structure with sync and backup option
type JsonFileData struct {
	path       string
	items      map[string]json.RawMessage
	lock       sync.RWMutex
	updatesync bool
}

//jsonFileManager - Holds global state of all collections
type jsonFileManager struct {
	path string
	m    map[string]*JsonFileData
	lock sync.Mutex
}

//FileManager - manages collections in the directory
type FileManager interface {
	Close()
	Path() string
	NewData(name string, tm int, updatesync bool) (*JsonFileData, error)
	GetData(name string, tm int, updatesync bool) (*JsonFileData, error)
}

var managers = map[string]FileManager{}

//GetFileManager - gets a manager for a given directory, if the directory is already owned by manager returns existing manager
func GetFileManager(path string) FileManager {

	for m, i := range managers {
		if m == path {
			return i
		}
	}
	m := &jsonFileManager{path: path, m: map[string]*JsonFileData{}, lock: sync.Mutex{}}
	managers[path] = m

	return m
}

//Close - sync closes all collections
func (cm *jsonFileManager) Close() {
	defer cm.lock.Unlock()
	cm.lock.Lock()

	for k, n := range cm.m {
		n.Sync()
		delete(cm.m, k)
	}
}

//Path - returns dircetory path
func (cm *jsonFileManager) Path() string {
	return cm.path
}

//KeyCollector - Collects results from insert multiple records
type KeyCollector interface {
	Collect(key string)
}

//NewData - creates a new store with optional sync every tm seconds and/or sync after insert/delete/update operations
func (cm *jsonFileManager) NewData(name string, tm int, updatesync bool) (*JsonFileData, error) {

	_, err := cm.getFileData(name, tm, updatesync, false)

	if err == errCollectionNotExists {
		return cm.createFileData(name, tm, updatesync)
	}

	return nil, errCollectionExists
}

//GetData - gets an existing store
func (cm *jsonFileManager) GetData(name string, tm int, updatesync bool) (*JsonFileData, error) {

	col, err := cm.getFileData(name, tm, updatesync, true)

	return col, err
}

//getFileData - Checks if file collection is already loaded into memory if not, checks if file exists.
// and if load is true, loads collection into memory
func (cm *jsonFileManager) getFileData(name string, tm int, updatesync, load bool) (*JsonFileData, error) {

	defer cm.lock.Unlock()
	cm.lock.Lock()

	fname := fmt.Sprintf("%s.json", name)
	fpath := filepath.Join(cm.path, fname)

	s, exists := cm.m[name]

	if !exists {

		_, err := os.Stat(fpath)

		if err != nil {
			return nil, errCollectionNotExists
		}
		//Prevents from loading collection
		if load {
			var data []byte
			var err error
			if data, err = ioutil.ReadFile(fpath); err != nil {
				return nil, err
			}

			items := map[string]json.RawMessage{}
			if err = json.Unmarshal(data, &items); err != nil {
				return nil, err
			}

			s = initialize(fpath, updatesync, items)
			cm.m[name] = s

			go watch(s, tm)
		}
	}

	return s, nil
}

//CreateCollection - creates a new collection only if file does not exists yet otherwise returns error
func (cm *jsonFileManager) createFileData(name string, tm int, updatesync bool) (*JsonFileData, error) {
	defer cm.lock.Unlock()
	cm.lock.Lock()

	fname := fmt.Sprintf("%s.json", name)
	fpath := filepath.Join(cm.path, fname)

	if _, exists := cm.m[name]; exists {
		return nil, errCollectionExists
	}

	s := initialize(fpath, updatesync, map[string]json.RawMessage{})
	cm.m[name] = s

	go watch(s, tm)

	return s, nil

}

//Insert - inserts a new item
func (s *JsonFileData) Insert(key string, item json.RawMessage) error {

	if s.updatesync {
		defer s.Sync()
	}

	defer s.lock.Unlock()
	s.lock.Lock()
	if _, ok := s.items[key]; !ok {
		s.items[key] = item
		return nil
	}

	return errKeyExists
}

//ForEach - this method helps load multiple records into collection. It takes a slice of elements
//that will be loaded into the collection, a function that will be performed for each element e.g. conversion to json format.
//The KeyCollector will collect ids of inserted elements
func (s *JsonFileData) ForEach(items []interface{}, kc KeyCollector, fn func(item interface{}) (string, []byte, error)) error {

	if s.updatesync {
		defer s.Sync()
	}
	defer s.lock.Unlock()
	s.lock.Lock()

	for n := range items {
		k, v, e := fn(items[n])
		if _, exists := s.items[k]; exists || e != nil {
			return e

		}
		s.items[k] = v
		kc.Collect(k)
	}

	return nil
}

//Get - gets an item from a store
func (s *JsonFileData) Get(key string) (json.RawMessage, bool) {

	defer s.lock.RUnlock()
	s.lock.RLock()

	item, ok := s.items[key]
	return item, ok
}

//Count - returns a total number of elements in a collection
func (s *JsonFileData) Count() int64 {

	defer s.lock.RUnlock()
	s.lock.RLock()

	return int64(len(s.items))
}

//Update - updates an item
func (s *JsonFileData) Update(key string, item json.RawMessage) error {

	if s.updatesync {
		defer s.Sync()
	}

	defer s.lock.Unlock()
	s.lock.Lock()
	if _, ok := s.items[key]; ok {
		s.items[key] = item
		return nil
	}

	return errKeyNotFound
}

//Delete - removes an item from a store
func (s *JsonFileData) Delete(key string) {

	if s.updatesync {
		defer s.Sync()
	}

	defer s.lock.Unlock()
	s.lock.Lock()
	delete(s.items, key)
}

//All - Returns all items in store
func (s *JsonFileData) All() []json.RawMessage {

	defer s.lock.RUnlock()
	s.lock.RLock()
	col := make([]json.RawMessage, len(s.items), len(s.items))
	i := 0
	for _, v := range s.items {
		col[i] = v
		i++
	}

	return col
}

//Sync - writes map to disk
func (s *JsonFileData) Sync() {

	tmp := map[string]interface{}{}
	s.lock.Lock()
	for k, v := range s.items {
		tmp[k] = v

	}
	s.lock.Unlock()

	result, _ := json.Marshal(tmp)

	ioutil.WriteFile(s.path, result, 0644)

}

func initialize(path string, updatesync bool, items map[string]json.RawMessage) *JsonFileData {

	s := &JsonFileData{path: path, items: items, lock: sync.RWMutex{}, updatesync: updatesync}

	return s
}

func watch(s *JsonFileData, tm int) {

	if tm == 0 {
		return
	}

	t := time.NewTicker(time.Duration(tm) * time.Second)
	for {
		select {
		case <-t.C:
			{
				s.Sync()
			}
		}
	}
}
