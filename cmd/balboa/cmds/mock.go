// balboa
// Copyright (c) 2019, DCSO GmbH

package cmds

import (
	db "balboa/backend/go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// mockCmd represents the `mock` command
var mockCmd = &cobra.Command{
	Use:   "mock",
	Short: "Mock backend",
	Long:  "Mock backend for debugging purposes",
	Run: func(cmd *cobra.Command, args []string) {
		log.Infof("running mock backend")
		var err error
		var verbose bool
		verbose, err = cmd.Flags().GetBool("verbose")
		if err != nil {
			log.Fatal(err)
		}
		if verbose {
			log.SetLevel(log.DebugLevel)
		}

		var host string
		host, err = cmd.Flags().GetString("host")
		if err != nil {
			log.Fatal(err)
		}

		db.Serve(host, nil)
	},
}

func init() {
	rootCmd.AddCommand(mockCmd)

	mockCmd.Flags().BoolP("verbose", "v", false, "verbose mode")
	mockCmd.Flags().StringP("host", "H", "localhost:4242", "listen host and port of the backend")
}
