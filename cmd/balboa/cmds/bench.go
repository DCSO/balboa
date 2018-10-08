// balboa
// Copyright (c) 2018, DCSO GmbH

package cmds

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/machinebox/graphql"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// benchCmd represents the bench command
var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run a number of queries against given endpoint",
	Long: `This command issues a potentially large number of queries against a
given GraphQL endpoint, for example for benchmarking. It does this in a concurrent
fashion.`,
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		var wg sync.WaitGroup
		rand.Seed(time.Now().Unix())

		var verbose bool
		verbose, err = cmd.Flags().GetBool("verbose")
		if err != nil {
			log.Fatal(err)
		}
		if verbose {
			log.SetLevel(log.DebugLevel)
		}

		var url string
		url, err = cmd.Flags().GetString("url")
		if err != nil {
			log.Fatal(err)
		}
		client := graphql.NewClient(url)

		var count int
		count, err = cmd.Flags().GetInt("count")
		if err != nil {
			log.Fatal(err)
		}

		ipChan := make(chan string, count)
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(mywg *sync.WaitGroup) {
				defer mywg.Done()
				req := graphql.NewRequest(`
			query ($ip: String!){
				entries(rdata: $ip) {
				rdata
				rrname
				rrtype
				time_first
				time_last
				count
				sensor_id
				}
			}`)
				req.Header.Set("Cache-Control", "no-cache")
				for ip := range ipChan {
					req.Var("ip", ip)
					var respData observationResultSet
					if err := client.Run(context.Background(), req, &respData); err != nil {
						log.Fatal(err)
					}
					if len(respData.Entries) > 0 {
						log.Info(respData)
					}
				}
			}(&wg)
		}

		for i := int(0); i < count; i++ {
			if i%1000 == 0 {
				log.Info(i)
			}
			ip := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(255), rand.Intn(255),
				rand.Intn(255), rand.Intn(255))
			ipChan <- ip
		}
		close(ipChan)
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(benchCmd)

	benchCmd.Flags().BoolP("verbose", "v", false, "verbose mode")
	benchCmd.Flags().StringP("url", "u", "http://localhost:8080/query", "URL of GraphQL interface to query")
	benchCmd.Flags().IntP("count", "c", 10000, "number of queries")
}
