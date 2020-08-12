// balboa
// Copyright (c) 2018, DCSO GmbH

package cmds

import (
	"github.com/DCSO/balboa/selector"
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

/*
 * TODO:
 *    * trap SIGHUP and reinitialize selectors
 */

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
		backend, err := cmd.Flags().GetString("backend")
		if err != nil {
			log.Fatal(err)
		}
		backendConfig, err := ioutil.ReadFile(backend)
		if err != nil {
			log.Fatalf("could not read backend configuration due to %v", err)
		}
		db.ObservationDB, err = db.MakeRemoteBackend(backendConfig, true)
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

		// Set up selectors from config file
		var selectorsFile string
		selectorsFile, err = cmd.Flags().GetString("selectors")
		if err != nil {
			log.Fatal(err)
		}
		cfgSelectors, err := ioutil.ReadFile(selectorsFile)
		selectorsEnabled := true
		if err != nil {
			log.Warnf("not using the selector subsystem due to %v", err)
			selectorsEnabled = false
		}

		var selectorOutChan chan observation.InputObservation
		var selectorEngine *selector.Engine
		if selectorsEnabled {
			selectorOutChan = make(chan observation.InputObservation)
			selectorEngine, err = selector.MakeSelectorEngine(cfgSelectors, selectorOutChan)
			if err != nil {
				log.Fatalf("could not instantiate selector engine due to %v", err)
			}
		}

		err = fsetup.Run(observation.InChan)
		if err != nil {
			log.Fatal(err)
		}

		// Start processing submissions
		consumeDone := make(chan bool, 1)
		go func() {
			for {
				select {
				case <-consumeDone:
					log.Infof("ConsumeFeed() done")
					return
				default:
					log.Infof("ConsumeFeed() starting")
					if selectorsEnabled {
						db.ObservationDB.ConsumeFeed(selectorOutChan)
					} else {
						db.ObservationDB.ConsumeFeed(observation.InChan)
					}
					log.Info("ConsumeFeed() finished")
					time.Sleep(10 * time.Second)
				}
			}
		}()

		if selectorsEnabled {
			selectorEngine.ConsumeFeed(observation.InChan)
		}

		// start GraphQL query server
		var port int
		port, err = cmd.Flags().GetInt("port")
		if err != nil {
			log.Fatal(err)
		}
		gql := query.GraphQLFrontend{}
		gql.Run(int(port))

		// start REST query server
		var rport int
		rport, err = cmd.Flags().GetInt("rest-port")
		if err != nil {
			log.Fatal(err)
		}
		if rport != 0 {
			rq := query.RESTFrontend{}
			rq.Run(int(rport))
		}

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
	serveCmd.Flags().StringP("feeders", "f", "feeders.yaml", "feeders configuration file")
	serveCmd.Flags().StringP("selectors", "s", "selectors.yaml", "selectors configuration file")
	serveCmd.Flags().IntP("port", "p", 8080, "port for GraphQL server")
	serveCmd.Flags().IntP("rest-port", "r", 8088, "port for REST server")
	serveCmd.Flags().StringP("logfile", "l", "/var/log/balboa.log", "log file path")
	serveCmd.Flags().BoolP("logjson", "j", true, "output log file as JSON")
	serveCmd.Flags().StringP("backend", "b", "backend.yaml", "backend configuration file")
}
