package config

import (
	"go-data-flow/pkg/flow"
	"go-data-flow/pkg/logs"
	"go-data-flow/pkg/redis"
	"go-data-flow/pkg/util"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	DaemonKey string            `yaml:"daemon_key"`
	Flows     []flow.Config     `yaml:"flows"`
	Redis     redis.RedisConfig `yaml:"redis"`
	Http      HttpConfig        `yaml:"http"`
	Log       logs.Config       `yaml:"log"`
	Mail      util.SMTPConfig   `yaml:"smtp"`
}

var Cfg Config

type HttpConfig struct {
	Addr string `yaml:"addr"`
}

func LoadConfig(fpath string) error {
	data, err := ioutil.ReadFile(fpath)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, &Cfg)
	if err != nil {
		return err
	}
	if Cfg.DaemonKey == "" {
		Cfg.DaemonKey = "go-data-flow-daemon"
	}
	logs.Init(Cfg.Log)
	util.IfErrPanic(redis.Init(Cfg.Redis))
	return nil
}
