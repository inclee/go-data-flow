package logs

import (
	"io"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Config struct {
	Level  string `yaml:"level"`
	Output string `yaml:"output"`
	UDP    string `yaml:"udp"`
}

func Init(config Config) error {
	setLogLevel(config.Level)
	writers := []io.Writer{}
	if config.Output == "stdout" {
		writers = append(writers, os.Stdout)
	} else {
		file, err := os.Create(config.Output)
		if err != nil {
			return err
		}
		defer file.Close()
		writers = append(writers, file)
	}
	if config.UDP != "" {
		writer, err := NewUDPWriter(config.UDP)
		if err != nil {
			return err
		}
		writers = append(writers, writer)
	}
	multiWriter := io.MultiWriter(writers...)
	// 设置日志输出
	log.Logger = zerolog.New(multiWriter).With().Any("app", "data-flow").Timestamp().Logger()
	zerolog.DefaultContextLogger = &log.Logger
	return nil
}

func setLogLevel(level string) {
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel) // 默认级别
	}
}
