package caskdb

import (
	"errors"
	"io/fs"
	"os"
	"time"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	file   *os.File
	keyDir map[string]KeyEntry
	offset uint32
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	file, err := os.Create(fileName)
	return &DiskStore{file, make(map[string]KeyEntry), 0}, err
}

func (d *DiskStore) Get(key string) string {
	keyInfo, found := d.keyDir[key]

	if !found {
		return ""
	}

	byteArray := make([]byte, keyInfo.TotalSize)

	_, err := d.file.Seek(int64(keyInfo.Position), 0)
	if err != nil {
		panic("Get() error during Seek")
	}

	_, err = d.file.Read(byteArray)
	if err != nil {
		panic("Get() error during Read")
	}

	_, _, value := decodeKV(byteArray)
	return string(value)
}

func (d *DiskStore) Set(key string, value string) {
	ts := uint32(time.Now().Unix())
	totalSize, byteArr := encodeKV(ts, key, value)
	d.keyDir[key] = NewKeyEntry(ts, d.offset, uint32(totalSize))
	d.offset += uint32(totalSize) + 1
	d.file.Write(byteArr)
}

func (d *DiskStore) Close() bool {
	err := d.file.Close()
	if err != nil {
		return false
	}

	return true
}
