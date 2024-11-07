package main

import (
	"fmt"
	"go-data-flow/cmd"
	"go-data-flow/internal/config"
	"log"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	// Retrieve the config flag from the root command after executing the command
	cobra.OnInitialize(initConfig)

	// Execute the root command
	if err := cmd.Root.Execute(); err != nil {
		log.Fatalf("command execution failed: %v", err)
		os.Exit(1)
	}
}

func initConfig() {
	// Retrieve the value of the config flag after command initialization
	cfpath, err := cmd.Root.PersistentFlags().GetString("config")
	if err != nil {
		log.Fatalf("Failed to retrieve config path: %v", err)
	}

	// Load the config file using the retrieved path
	if err := config.LoadConfig(cfpath); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Println("Config loaded from:", cfpath)
}
