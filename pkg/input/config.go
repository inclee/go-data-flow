package input

import "go-data-flow/pkg/input/canal"

type Config struct {
	Canal *canal.Config     `yaml:"canal"`
	Kafka *KafkaInputConfig `yaml:"kafka"`
}
