package maple_juice_service

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type MJConf struct {
	Port      string `yaml:"port"`
	FilePath  string `yaml:"file_path"`
	AppPath   string `yaml:"app_path"`
	InputFile string `yaml:"input_file"`
}

func NewMJConf() MJConf {
	var c MJConf
	yamlFile, err := ioutil.ReadFile("conf_maplejuice.yaml")
	if err != nil {
		log.Fatal("ERROR get yaml file: %v", err)
	}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		log.Fatal("ERROR unmarshal: %v", err)
	}
	return c
}