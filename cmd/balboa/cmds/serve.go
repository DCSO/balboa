// balboa
// Copyright (c) 2018, DCSO GmbH

package cmds

import (
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DCSO/balboa/db"
	"github.com/DCSO/balboa/feeder"
	"github.com/DCSO/balboa/observation"
	"github.com/DCSO/balboa/query"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run the balboa frontend",
	Long: `This command starts the balboa frontend, accepting submissions and
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

		// connect to backend
		host, err := cmd.Flags().GetString("host")
		db.ObservationDB, err = db.MakeRemoteBackend(host, true)
		if err != nil {
			log.Fatal(err)
		}

		// Set up feeders from config file
		var feedersFile string
		feedersFile, err = cmd.Flags().GetString("feeders")
		if err != nil {
			log.Fatal(err)
		}
		cfgYaml, err := ioutil.ReadFile(feedersFile)
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
		consumeDone := make(chan bool, 1)
		go func () {
			for {
				select {
					case <-consumeDone:
						log.Warnf("ConsumeFeed() done")
						return
					default:
						log.Warnf("ConsumeFeed() starting")
						db.ObservationDB.ConsumeFeed(observation.InChan)
						log.Warnf("ConsumeFeed() finished")
						time.Sleep(10*time.Second)
				}
			}
		}()

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
			close(consumeDone)
			stopChan := make(chan bool)
			fsetup.Stop(stopChan)
			<-stopChan
			db.ObservationDB.Shutdown()
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
	serveCmd.Flags().StringP("host", "H", "127.0.0.1:4242", "remote database host and port")
}
