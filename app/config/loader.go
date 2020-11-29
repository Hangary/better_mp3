package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

type MemberServiceConfig struct {
	IntroducerIP   string        `yaml:"introducer_ip"`
	Port           string        `yaml:"port"`
	GossipInterval time.Duration `yaml:"gossip_interval"`
	SuspectTime    int           `yaml:"suspect_time"`
	FailTime       int           `yaml:"fail_time"`
	RemoveTime     int           `yaml:"remove_time"`
}

type MapleJuiceServiceConfig struct {
	Port      string `yaml:"port"`
	FilePath  string `yaml:"file_path"`
	AppPath   string `yaml:"app_path"`
	InputFile string `yaml:"input_file"`
}

type FileServiceConfig struct {
	Port string `yaml:"port"`
	Path string `yaml:"path"`
}

type Config struct {
	Debug      string `yaml:"debug"`
	BufferSize  string `yaml:"buffer_size"`

	MemberServiceConfig     MemberServiceConfig `yaml:"member_service"`
	FileServiceConfig       FileServiceConfig `yaml:"file_service"`
	MapleJuiceServiceConfig MapleJuiceServiceConfig `yaml:"maplejuice_service"`
}

var config Config

func LoadConfig(configFilePath string) {
	yamlFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		fmt.Println(err)
	}
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		fmt.Println(err)
	}
}

func GetConfig() Config {
	return config
}

func GetMemberServiceConfig() MemberServiceConfig {
	return config.MemberServiceConfig
}

func GetMapleJuiceServiceConfig() MapleJuiceServiceConfig {
	return config.MapleJuiceServiceConfig
}

func GetFileServiceConfig() FileServiceConfig {
	return config.FileServiceConfig
}