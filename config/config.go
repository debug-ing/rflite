package config

import (
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Name string `yaml:"name"`
	Port int    `yaml:"port"`
	Type string `yaml:"type"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
