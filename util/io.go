package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

func ListDir(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, f := range files {
		names = append(names, f.Name())
	}
	return names, nil
}

func AtomicFileSwap(fileName string, content []byte) error {
	tmpFile, err := os.CreateTemp("", "*.tmp.raft")
	if err != nil {
		return fmt.Errorf("creating tmp file: %w", err)
	}
	defer func() {
		_ = os.Remove(tmpFile.Name())
	}()

	if _, err = tmpFile.Write(content); err != nil {
		return fmt.Errorf("writing to tmp file: %w", err)
	}

	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("syncing tmp file: %w", err)
	}

	if err = os.Rename(tmpFile.Name(), fileName); err != nil {
		return fmt.Errorf("attomicaly changing contents of the file: %w", err)
	}

	// fsync the parent directory too to make sure newly created file is really persisted
	// ref: https://lwn.net/Articles/457667/
	dir, err := os.Open(path.Dir(fileName))
	if err != nil {
		return fmt.Errorf("opening parent dir: %w", err)
	}

	if err = dir.Sync(); err != nil {
		return fmt.Errorf("syncing parent dir: %w", err)
	}

	return nil
}

func CreateDirIfNotExists(dir string) error {
	return os.MkdirAll(dir, os.ModePerm)
}
