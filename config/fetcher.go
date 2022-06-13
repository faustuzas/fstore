package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path"
)

const (
	_defaultConfigDir = `config`
)

type Fetcher struct {
	configDir string
}

func NewFetcher(configDir string) (Fetcher, error) {
	stat, err := os.Stat(configDir)
	if err != nil {
		return Fetcher{}, fmt.Errorf("checking config dir existance: %w", err)
	}

	if !stat.IsDir() {
		return Fetcher{}, fmt.Errorf("%s is not a directory", configDir)
	}

	return Fetcher{
		configDir: configDir,
	}, nil
}

func NewDefaultFetcher() (Fetcher, error) {
	return NewFetcher(_defaultConfigDir)
}

func (f Fetcher) fillFromFile(configFileName string, config interface{}) error {
	p := path.Join(f.configDir, configFileName)

	content, err := ioutil.ReadFile(p)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(content, config)
}