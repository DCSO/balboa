// balboa
// Copyright (c) 2018, DCSO GmbH

package cmds

import (
	"github.com/DCSO/balboa/db"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var rocksdumpCmd = &cobra.Command{
	Use:   "rocksdump [netmask]",
	Short: "Dump information from RocksDB",
	Long:  `This command allows the user to directly dump bulk information from RocksDB.`,
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		if len(args) == 0 {
			log.Fatal("needs database path as argument")
		}

		var verbose bool
		verbose, err = cmd.Flags().GetBool("verbose")
		if err != nil {
			log.Fatal(err)
		}
		if verbose {
			log.SetLevel(log.DebugLevel)
		}

		rdb, err := db.MakeRocksDBReadonly(args[0])
		if err != nil {
			log.Fatal(err)
		}

		rdb.Dump()
	},
}

func init() {
	rootCmd.AddCommand(rocksdumpCmd)

	rocksdumpCmd.Flags().BoolP("verbose", "v", false, "verbose mode")
}
