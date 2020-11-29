package file_service

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type FileServiceConfig struct {
	Port string `yaml:"port"`
	Path string `yaml:"path"`
}

func GetFileServiceConfig() FileServiceConfig {
	var c FileServiceConfig
	yamlFile, err := ioutil.ReadFile("conf_server.yaml")
	if err != nil {
		log.Fatal("ERROR get yaml file: %v", err)
	}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		log.Fatal("ERROR unmarshal: %v", err)
	}
	return c
}
