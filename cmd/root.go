package cmd

import (
	"github.com/spf13/cobra"
)

var Root = &cobra.Command{
	Use:   "binlog-flow",
	Short: "Stream MySQL binlog to a different output.",
}

func init() {
	// 在 root 命令上定义 config flag 并设置默认值
	Root.PersistentFlags().String("config", "config.yaml", "path to config file")
}
