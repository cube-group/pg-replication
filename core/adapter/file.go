package adapter

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
)

func NewLsnFileAdapter(dir string) LsnAdapter {
	return &FileLsnAdapter{dir: dir}
}

type FileLsnAdapter struct {
	dir string
	LsnAdapter
}

func (t *FileLsnAdapter) Close() error {
	return nil
}

func (t *FileLsnAdapter) name(key string) string {
	return filepath.Join(t.dir, key+".db")
}

func (t *FileLsnAdapter) Set(key string, value uint64) error {
	return ioutil.WriteFile(t.name(key), []byte(fmt.Sprintf("%v", value)), 0777)
}

func (t *FileLsnAdapter) Get(key string) (uint64, error) {
	bytes, err := ioutil.ReadFile(t.name(key))
	if err != nil {
		return 0, err
	}
	res, err := strconv.ParseInt(string(bytes), 0, 64)
	if err != nil {
		return 0, err
	}
	return uint64(res), nil
}
