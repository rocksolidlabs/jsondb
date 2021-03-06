package jsondb

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/intwinelabs/logger"
	"github.com/rocksolidlabs/jsonq"
)

const Version = "0.0.1"

type JSONDB struct {
	Trace   bool
	Logger  *logger.Logger
	Dir     string
	mutex   sync.Mutex
	mutexes map[string]sync.Mutex
}

// Create a new JSONDB instance using os FS
func NewJSONDB(datadir string, log *logger.Logger, trace bool) (*JSONDB, error) {

	dir := filepath.Clean(datadir + "/db")

	// create db struct
	db := &JSONDB{
		Trace:   trace,
		Logger:  log,
		Dir:     dir,
		mutexes: make(map[string]sync.Mutex),
	}

	// if the database already exists, just use it
	if _, err := os.Stat(dir); err == nil {
		if trace {
			db.Logger.Info("Using '%s' (database already exists)\n", dir)
		}
		return db, nil
	}

	// if the database doesn't exist create it
	if trace {
		db.Logger.Info("Creating database at '%s'...\n", dir)
	}
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// InitCollection creates a collection in the JSONDB
func (db *JSONDB) InitCollection(collection string) error {
	mutex := db.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	// path for the collection
	dir := filepath.Join(db.Dir, collection)

	// create collection directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return nil
}

// Put locks the database and attempts to write the record to the database under
// the [collection] specified with the [resource] name given
func (db *JSONDB) Put(collection, resource string, v interface{}) error {

	// ensure there is a place to save record
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	// ensure there is a resource (name) to save record as
	if resource == "" {
		resource = GenID(64)
	}

	mutex := db.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	//
	dir := filepath.Join(db.Dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	// create collection directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	//
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	// write marshaled data to the temp file
	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	// move final file into place
	return os.Rename(tmpPath, fnlPath)
}

// Get a record from the database and marshal it to the passed object
func (db *JSONDB) Get(collection, resource string, record interface{}) error {

	// ensure there is a place to save record
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	// ensure there is a resource (name) to save record as
	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record (no name)!")
	}

	//
	rec := filepath.Join(db.Dir, collection, resource)

	// check to see if file exists
	if _, err := db.Stat(rec); err != nil {
		return err
	}

	// read record from database
	b, err := ioutil.ReadFile(rec+".json")
	if err != nil {
		return err
	}

	// unmarshal data
	return json.Unmarshal(b, &record)
}

// Get a record from the database and return the JSON byte array
func (db *JSONDB) GetBytes(collection, resource string) ([]byte, error) {

	// ensure there is a place to save record
	if collection == "" {
		return nil, fmt.Errorf("Missing collection - no place to save record!")
	}

	// ensure there is a resource (name) to save record as
	if resource == "" {
		return nil, fmt.Errorf("Missing resource - unable to save record (no name)!")
	}

	//
	rec := filepath.Join(db.Dir, collection, resource)

	// check to see if file exists
	if _, err := db.Stat(rec); err != nil {
		return nil, err
	}

	// read record from database
	b, err := ioutil.ReadFile(rec+".json")
	if err != nil {
		return nil, err
	}

	// unmarshal data
	return b, nil
}

// GetWhere records from a collection where the query path equals the expression;
// this populates the passed slice with records
func (db *JSONDB) GetWhere(collection, query string, expression, records interface{}, limit int64) error {

	// ensure there is a collection to read
	if collection == "" {
		return fmt.Errorf("Missing collection - unable to record location!")
	}

	//
	dir := filepath.Join(db.Dir, collection)

	// check to see if collection (directory) exists
	if _, err := db.Stat(dir); err != nil {
		return err
	}

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, _ := ioutil.ReadDir(dir)

	// get the type of the records
	rt, err := toSliceType(records)
	if err != nil {
		return err
	}

	// number of records found
	var numFound int64 = 0

	// iterate over each of the files, attempting to read the file. If successful
	// append the cast records to the passed collection slice
	for _, file := range files {
		// read the file bytes
		fileBytes, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return err
		}
		// check the query to see if the record matches
		data := map[string]interface{}{}
		dec := json.NewDecoder(strings.NewReader(string(fileBytes)))
		dec.Decode(&data)
		jq := jsonq.NewQuery(data)
		var queryPath []string
		if strings.Index(query, ".") > -1 {
			queryPath = strings.Split(query, ".")
		} else {
			queryPath = append(queryPath, query)
		}
		var val interface{}
		var match bool
		switch expression.(type) {
		case int:
			val, err = jq.Int(queryPath...)
			match = val == expression.(int)
		case float64:
			val, err = jq.Float(queryPath...)
			match = val == expression.(float64)
		case string:
			val, err = jq.String(queryPath...)
			match = val == expression.(string)
		case bool:
			val, err = jq.Bool(queryPath...)
			match = val == expression.(bool)
		case interface{}:
			val, err = jq.Interface(queryPath...)
			match = val == expression.(interface{})
		}
		if err != nil {
			return err
		} else if match {

			// create a new record of the type of the passed slice elements
			// and Unmarshal the JSON bytes from the file
			record := reflect.New(rt)
			recordInterface := record.Interface()
			err = json.Unmarshal(fileBytes, &recordInterface)
			if err != nil {
				return err
			}
			// we append the new records to the passed slice
			recordsVal := reflect.ValueOf(records).Elem()
			recordsVal.Set(reflect.Append(recordsVal, record.Elem()))

			// we found a record so now we increment numFound
			numFound++
		}
		// check limit
		if numFound == limit {
			break
		}
	}

	return nil
}

// GetWhereNot records from a collection where the query path does not equal the expression;
// this populates the passed slice with records
func (db *JSONDB) GetWhereNot(collection, query string, expression, records interface{}, limit int64) error {

	// ensure there is a collection to read
	if collection == "" {
		return fmt.Errorf("Missing collection - unable to record location!")
	}

	//
	dir := filepath.Join(db.Dir, collection)

	// check to see if collection (directory) exists
	if _, err := db.Stat(dir); err != nil {
		return err
	}

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, _ := ioutil.ReadDir(dir)

	// get the type of the records
	rt, err := toSliceType(records)
	if err != nil {
		return err
	}

	// number of records found
	var numFound int64 = 0

	// iterate over each of the files, attempting to read the file. If successful
	// append the cast records to the passed collection slice
	for _, file := range files {
		// read the file bytes
		fileBytes, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return err
		}
		// check the query to see if the record matches
		data := map[string]interface{}{}
		dec := json.NewDecoder(strings.NewReader(string(fileBytes)))
		dec.Decode(&data)
		jq := jsonq.NewQuery(data)
		var queryPath []string
		if strings.Index(query, ".") > -1 {
			queryPath = strings.Split(query, ".")
		} else {
			queryPath = append(queryPath, query)
		}
		var val interface{}
		var match bool
		switch expression.(type) {
		case int:
			val, err = jq.Int(queryPath...)
			match = val == expression.(int)
		case float64:
			val, err = jq.Float(queryPath...)
			match = val == expression.(float64)
		case string:
			val, err = jq.String(queryPath...)
			match = val == expression.(string)
		case bool:
			val, err = jq.Bool(queryPath...)
			match = val == expression.(bool)
		case interface{}:
			val, err = jq.Interface(queryPath...)
			match = val == expression.(interface{})
		}
		if err != nil {
			return err
		} else if !match {
			// create a new record of the type of the passed slice elements
			// and Unmarshal the JSON bytes from the file
			record := reflect.New(rt)
			recordInterface := record.Interface()
			err = json.Unmarshal(fileBytes, &recordInterface)
			if err != nil {
				return err
			}
			// we append the new records to the passed slice
			recordsVal := reflect.ValueOf(records).Elem()
			recordsVal.Set(reflect.Append(recordsVal, record.Elem()))

			// we found a record so now we increment numFound
			numFound++
		}
		// check limit
		if numFound == limit {
			break
		}
	}

	return nil
}

// GetAll records from a collection; this populates the passed slice with records
func (db *JSONDB) GetAll(collection string, records interface{}) error {

	// ensure there is a collection to read
	if collection == "" {
		return fmt.Errorf("Missing collection - unable to record location!")
	}

	//
	dir := filepath.Join(db.Dir, collection)

	// check to see if collection (directory) exists
	if _, err := db.Stat(dir); err != nil {
		db.Logger.Errorf("Error: %+v", err)
		return err
	}

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, _ := ioutil.ReadDir(dir)

	// get the type of the records
	rt, err := toSliceType(records)
	if err != nil {
		db.Logger.Errorf("Error: %+v", err)
		return err
	}

	// iterate over each of the files, attempting to read the file. If successful
	// append the cast records to the passed collection slice
	for _, file := range files {
		// read the file bytes
		fileBytes, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			db.Logger.Errorf("Error: %+v", err)
			return err
		}
		// create a new record of the type of the passed slice elements
		// and Unmarshal the JSON bytes from the file
		record := reflect.New(rt)
		recordInterface := record.Interface()
		err = json.Unmarshal(fileBytes, &recordInterface)
		if err != nil {
			db.Logger.Errorf("Error: %+v", err)
			return err
		}
		// we append the new records to the passed slice
		recordsVal := reflect.ValueOf(records).Elem()
		recordsVal.Set(reflect.Append(recordsVal, record.Elem()))
	}

	return nil
}

// Delete locks that database and then attempts to remove the collection/resource
// specified by [path]
func (db *JSONDB) Delete(collection, resource string) error {
	path := filepath.Join(collection, resource)

	// lcok the JSONDB
	mutex := db.getOrCreateMutex(path)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(db.Dir, path)

	switch fi, err := db.Stat(dir); {

	// if fi is nil or error is not nil return
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find file or directory named %v\n", path)

	// remove directory and all contents
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	// remove file
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}

	return nil
}

// Link locks that database and then creates a link to a existing resource
func (db *JSONDB) Link(srcCollection, srcResource, destCollection, destResource string) error {
	srcPath := filepath.Join(srcCollection, srcResource)
	destPath := filepath.Join(destCollection, destResource)

	// lock the JSONDB
	mutex := db.getOrCreateMutex(srcPath)
	mutex.Lock()
	defer mutex.Unlock()

	src := filepath.Join(db.Dir, srcPath)
	dest := filepath.Join(db.Dir, destPath)

	switch fi, err := db.Stat(src); {

	// if fi is nil or error is not nil return
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find file or directory named %v\n", srcPath)

	// link a collection
	case fi.Mode().IsDir():
		return os.Symlink(src, dest)

	// link file
	case fi.Mode().IsRegular():
		os.Remove(dest + ".json")
		return os.Symlink(src+".json", dest+".json")

	}

	return nil
}

//
func (db *JSONDB) Stat(path string) (fi os.FileInfo, err error) {

	// check for dir, if path isn't a directory check to see if it's a file
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}

	return
}

// getOrCreateMutex creates a new collection specific mutex any time a collection
// is being modfied to avoid unsafe operations
func (db *JSONDB) getOrCreateMutex(collection string) sync.Mutex {

	db.mutex.Lock()
	defer db.mutex.Unlock()

	m, ok := db.mutexes[collection]

	// if the mutex doesn't exist make it
	if !ok {
		m = sync.Mutex{}
		db.mutexes[collection] = m
	}

	return m
}

// Simple ID Generating function
func GenID(idSize int) string {
	// dictinary of runes to use in ID
	dictionary := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	var bytes = make([]byte, idSize)
	rand.Read(bytes)
	for k, v := range bytes {
		bytes[k] = dictionary[v%byte(len(dictionary))]
	}
	return string(bytes)
}

// Simple ID Generating function from seed
func GenIDFromSeeed(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return fmt.Sprintf("%x", sum)
}

func toSliceType(i interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		// If it's a slice, return a more helpful error message
		if t.Kind() == reflect.Slice {
			return nil, fmt.Errorf("JSONDB: cannot cast into a non-pointer slice: %v", t)
		}
		return nil, nil
	}
	if t = t.Elem(); t.Kind() != reflect.Slice {
		return nil, nil
	}
	return t.Elem(), nil
}

func toType(i interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(i)

	// If a Pointer to a type, follow
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("JSONDB: cannot SELECT into this type: %v", reflect.TypeOf(i))
	}
	return t, nil
}

func cloneValue(source interface{}, destin interface{}) {
	x := reflect.ValueOf(source)
	if x.Kind() == reflect.Ptr {
		starX := x.Elem()
		y := reflect.New(starX.Type())
		starY := y.Elem()
		starY.Set(starX)
		reflect.ValueOf(destin).Elem().Set(y.Elem())
	} else {
		destin = x.Interface()
	}
}

func makeSlice(elemType reflect.Type) interface{} {
	if elemType.Kind() == reflect.Slice {
		elemType = elemType.Elem()
	}
	sliceType := reflect.SliceOf(elemType)
	slice := reflect.New(sliceType)
	slice.Elem().Set(reflect.MakeSlice(sliceType, 0, 0))
	return slice.Interface()
}

func indirect(reflectValue reflect.Value) reflect.Value {
	for reflectValue.Kind() == reflect.Ptr {
		reflectValue = reflectValue.Elem()
	}
	return reflectValue
}

func convert(i interface{}, protoType interface{}) interface{} {
	return convertType(i, reflect.TypeOf(protoType))
}

func convertType(i interface{}, typ reflect.Type) interface{} {
	return reflect.ValueOf(i).Convert(typ).Interface()
}