// balboa
// Copyright (c) 2018, DCSO GmbH

package cmds

import (
	"fmt"
	"os"
	"os/user"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "balboa",
	Short: "BAsic Little Book Of Answers",
}

// Execute runs the root command's Execute method as the Execute
// hook for the whole package.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
}

func initConfig() {
	usr, _ := user.Current()
	homedir := usr.HomeDir

	// Search config in home directory with name ".balboa" (without extension).
	viper.AddConfigPath(homedir)
	viper.SetConfigName(".balboa")

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
