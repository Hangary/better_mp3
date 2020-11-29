package member_service

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"time"
)

type MemberServiceConfig struct {
	Introducer     string        `yaml:"introducer"`
	Port           string        `yaml:"port"`
	GossipInterval time.Duration `yaml:"gossip_interval"`
	SuspectTime    int           `yaml:"suspect_time"`
	FailTime       int           `yaml:"fail_time"`
	RemoveTime     int           `yaml:"remove_time"`
}


func GetMemberServiceConfig() MemberServiceConfig {
	var c MemberServiceConfig
	yamlFile, err := ioutil.ReadFile("conf_daemon.yaml")
	if err != nil {
		log.Fatal("ERROR get yaml file: %v", err)
	}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		log.Fatal("ERROR unmarshal: %v", err)
	}
	return c
}