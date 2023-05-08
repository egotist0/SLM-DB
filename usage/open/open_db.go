package main

import (
	"io/ioutil"
	"os"
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
		_ = os.RemoveAll(path)
	}()

	if err != nil {
		panic(err)
	}
}
