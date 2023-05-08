package main

import (
	"io/ioutil"
	"storage"
)

func main() {
	// open a db with default options.
	path, _ := ioutil.TempDir("", "db")
	// you must specify a db path.
	opts := storage.DefaultOptions(path)
	db, err := storage.Open(opts)
	defer func() {
		_ = db.Close()
	}()

	if err != nil {
		panic(err)
	}
	cfOpts := storage.DefaultColumnFamilyOptions("a-new-cf")
	cfOpts.DirPath = "/tmp"
	cf, err := db.OpenColumnFamily(cfOpts)
	if err != nil {
		panic(err)
	}

	// the same with db.Put
	err = cf.Put([]byte("name"), []byte("egotist"))
	if err != nil {
		// ...
	}
}
