// balboa
// Copyright (c) 2018, DCSO GmbH

package cmds

import (
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/DCSO/balboa/db"
	"github.com/DCSO/balboa/feeder"
	"github.com/DCSO/balboa/observation"
	"github.com/DCSO/balboa/query"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run the balboa server",
	Long: `This command starts the balboa server, accepting submissions and 
answering queries.`,
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		// handle verbosity
		var verbose bool
		verbose, err = cmd.Flags().GetBool("verbose")
		if err != nil {
			log.Fatal(err)
		}
		if verbose {
			log.SetLevel(log.DebugLevel)
		}

		// handle logfile preferences
		var logfile string
		logfile, err = cmd.Flags().GetString("logfile")
		if err != nil {
			log.Fatal(err)
		}
		if logfile != "" {
			lf, err := os.OpenFile(logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				log.Fatal(err)
			}
			log.Infof("switching to log file %s", logfile)
			defer lf.Close()
			log.SetFormatter(&log.TextFormatter{
				DisableColors: true,
				FullTimestamp: true,
			})
			log.SetOutput(lf)
			var logjson bool
			logjson, err = cmd.Flags().GetBool("logjson")
			if err != nil {
				log.Fatal(err)
			}
			if logjson {
				log.SetFormatter(&log.JSONFormatter{})
			}
		}

		// Set up database from config file
		var dbFile string
		dbFile, err = cmd.Flags().GetString("dbconfig")
		if err != nil {
			log.Fatal(err)
		}
		cfgYaml, err := ioutil.ReadFile(dbFile)
		if err != nil {
			log.Fatal(err)
		}
		dbsetup, err := db.LoadSetup(cfgYaml)
		if err != nil {
			log.Fatal(err)
		}
		db.ObservationDB, err = dbsetup.Run()
		if err != nil {
			log.Fatal(err)
		}

		// Set up feeders from config file
		var feedersFile string
		feedersFile, err = cmd.Flags().GetString("feeders")
		if err != nil {
			log.Fatal(err)
		}
		cfgYaml, err = ioutil.ReadFile(feedersFile)
		if err != nil {
			log.Fatal(err)
		}
		fsetup, err := feeder.LoadSetup(cfgYaml)
		if err != nil {
			log.Fatal(err)
		}
		err = fsetup.Run(observation.InChan)
		if err != nil {
			log.Fatal(err)
		}

		// Start processing submissions
		go db.ObservationDB.ConsumeFeed(observation.InChan)

		// start query server
		var port int
		port, err = cmd.Flags().GetInt("port")
		if err != nil {
			log.Fatal(err)
		}
		gql := query.GraphQLFrontend{}
		gql.Run(int(port))

		sigChan := make(chan os.Signal, 1)
		done := make(chan bool, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			sig := <-sigChan
			log.Infof("received '%v' signal, shutting down", sig)
			stopChan := make(chan bool)
			fsetup.Stop(stopChan)
			<-stopChan
			stopChan = make(chan bool)
			dbsetup.Stop(stopChan)
			<-stopChan
			gql.Stop()
			close(done)
		}()
		<-done
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().BoolP("verbose", "v", false, "verbose mode")
	serveCmd.Flags().StringP("dbconfig", "d", "database.yaml", "database configuration file")
	serveCmd.Flags().StringP("feeders", "f", "feeders.yaml", "feeders configuration file")
	serveCmd.Flags().IntP("port", "p", 8080, "port for GraphQL server")
	serveCmd.Flags().StringP("logfile", "l", "/var/log/balboa.log", "log file path")
	serveCmd.Flags().BoolP("logjson", "j", true, "output log file as JSON")
}
