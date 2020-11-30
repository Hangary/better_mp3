package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
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
	Port     string `yaml:"port"`
	SdfsDir  string `yaml:"sdfs_dir"`
	TmpDir   string `yaml:"tmp_dir"`
	InputDir string `yaml:"input_dir"`
	ExecDir  string `yaml:"exec_dir"`
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

func CreateDir() {
	_ = os.Mkdir(config.MapleJuiceServiceConfig.TmpDir, PERM_MODE)
	_ = os.Mkdir(config.MapleJuiceServiceConfig.SdfsDir, PERM_MODE)
}

func LoadConfig(configFilePath string) {
	yamlFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		fmt.Println(err)
	}
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		fmt.Println(err)
	}
	CreateDir()
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