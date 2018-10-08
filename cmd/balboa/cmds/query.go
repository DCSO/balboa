// balboa
// Copyright (c) 2018, DCSO GmbH

package cmds

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/machinebox/graphql"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type observationResultSet struct {
	Entries []observationResult
}

type observationResult struct {
	Count     int    `json:"count"`
	TimeFirst int    `json:"time_first"`
	TimeLast  int    `json:"time_last"`
	RRType    string `json:"rrtype"`
	RRName    string `json:"rrname"`
	RData     string `json:"rdata"`
	SensorID  string `json:"sensor_id"`
}

func incIP(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}

func hosts(cidr string) ([]string, error) {
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, err
	}

	var ips []string
	for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); incIP(ip) {
		ips = append(ips, ip.String())
	}

	// remove network address and broadcast address
	if len(ips) > 2 {
		return ips[1 : len(ips)-1], nil
	}
	return ips, nil
}

var queryCmd = &cobra.Command{
	Use:   "query [netmask]",
	Short: "Obtain information from pDNS about IP ranges",
	Long: `This command allows to query a balboa endpoint for information regarding
IPs from a given range.`,
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		if len(args) == 0 {
			log.Fatal("needs network as argument, e.g. '192.168.0.0/24'")
		}

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

		var senid string
		senid, err = cmd.Flags().GetString("sensor")
		if err != nil {
			log.Fatal(err)
		}

		var wg sync.WaitGroup
		ipChan := make(chan string, 1000)
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(mywg *sync.WaitGroup) {
				defer mywg.Done()
				req := graphql.NewRequest(`
					query ($ip: String!, $senid: String){
						entries(rdata: $ip, sensor_id: $senid) {
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
					if senid != "" {
						req.Var("senid", senid)
					}
					var respData observationResultSet
					if err := client.Run(context.Background(), req, &respData); err != nil {
						log.Fatal(err)
					}
					if len(respData.Entries) > 0 {
						for _, entry := range respData.Entries {
							out, err := json.Marshal(entry)
							if err != nil {
								log.Fatal(err)
							}
							fmt.Println(string(out))
						}
					}
				}
			}(&wg)
		}

		hosts, err := hosts(args[0])
		if err != nil {
			log.Fatal(err)
		}
		for _, host := range hosts {
			ipChan <- host
		}
		close(ipChan)
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)

	queryCmd.Flags().BoolP("verbose", "v", false, "verbose mode")
	queryCmd.Flags().StringP("url", "u", "http://localhost:8080/query", "URL of GraphQL interface to query")
	queryCmd.Flags().StringP("sensor", "s", "", "limit query to observations from single sensor")
}
