// balboa
// Copyright (c) 2018, DCSO GmbH

package feeder

import (
	"fmt"
	"strings"

	"balboa/format"
	"balboa/observation"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

// Setup describes a collection of feeders that should be active, including
// their configuration settings.
type Setup struct {
	Feeder []struct {
		Name        string `yaml:"name"`
		Type        string `yaml:"type"`
		InputFormat string `yaml:"input_format"`
		// for AMQP
		URL      string   `yaml:"url"`
		Exchange []string `yaml:"exchange"`
		// for HTTP etc.
		ListenHost string `yaml:"listen_host"`
		ListenPort int    `yaml:"listen_port"`
		// for socket input
		Path string `yaml:"path"`
	} `yaml:"feeder"`
	Feeders map[string]Feeder
}

// LoadSetup creates a new Setup from a byte array containing YAML.
func LoadSetup(in []byte) (*Setup, error) {
	var fs Setup
	seenFeeders := make(map[string]bool)
	err := yaml.Unmarshal(in, &fs)
	if err != nil {
		return nil, err
	}
	fs.Feeders = make(map[string]Feeder)
	for _, f := range fs.Feeder {
		if f.Name == "" {
			return nil, fmt.Errorf("name missing")
		}
		if _, ok := seenFeeders[f.Name]; ok {
			return nil, fmt.Errorf("duplicate name: %s", f.Name)
		}
		seenFeeders[f.Name] = true
		if f.Type == "" {
			return nil, fmt.Errorf("type missing")
		}
		if f.InputFormat == "" {
			return nil, fmt.Errorf("input format missing")
		}
		switch f.Type {
		case "amqp":
			if len(f.Exchange) == 0 {
				return nil, fmt.Errorf("%s: Exchange missing", f.Name)
			}
			if f.URL == "" {
				return nil, fmt.Errorf("%s: URL missing", f.Name)
			}
		case "http":
			if f.ListenHost == "" {
				return nil, fmt.Errorf("%s: ListenHost missing", f.Name)
			}
			if f.ListenPort == 0 {
				return nil, fmt.Errorf("%s: ListenPort missing", f.Name)
			}
		case "socket":
			if f.Path == "" {
				return nil, fmt.Errorf("%s: socket Path missing", f.Name)
			}
		}
	}
	return &fs, nil
}

// Run starts all feeders according to the description in the setup, in the
// background. Use Stop() to stop the feeders.
func (fs *Setup) Run(in chan observation.InputObservation) error {
	for _, v := range fs.Feeder {
		log.Infof("starting feeder %s", v.Name)
		switch v.Type {
		case "amqp":
			queueName := strings.ToLower(strings.Replace(v.Name, " ", "_", -1))
			fs.Feeders[v.Name] = MakeAMQPFeeder(v.URL, v.Exchange, queueName)
			fs.Feeders[v.Name].Run(in)
		case "http":
			fs.Feeders[v.Name] = MakeHTTPFeeder(v.ListenHost, v.ListenPort)
			fs.Feeders[v.Name].Run(in)
		case "socket":
			f, err := MakeSocketFeeder(v.Path)
			if err != nil {
				return err
			}
			fs.Feeders[v.Name] = f
			fs.Feeders[v.Name].Run(in)
		}
		switch v.InputFormat {
		case "fever_aggregate":
			fs.Feeders[v.Name].SetInputDecoder(format.MakeFeverAggregateInputObservations)
		case "gopassivedns":
			fs.Feeders[v.Name].SetInputDecoder(format.MakeGopassivednsInputObservations)
		case "packetbeat":
			fs.Feeders[v.Name].SetInputDecoder(format.MakePacketbeatInputObservations)
		case "suricata_dns":
			fs.Feeders[v.Name].SetInputDecoder(format.MakeSuricataInputObservations)
		case "gamelinux":
			fs.Feeders[v.Name].SetInputDecoder(format.MakeFjellskaalInputObservations)
		default:
			log.Fatalf("unknown input format: %s", v.InputFormat)
		}
	}
	return nil
}

// Stop causes all feeders described in the setup to cease processing input.
// The stopChan will be closed once all feeders are done shutting down.
func (fs *Setup) Stop(stopChan chan bool) {
	for k, v := range fs.Feeders {
		log.Infof("stopping feeder %s", k)
		myStopChan := make(chan bool)
		v.Stop(myStopChan)
		<-myStopChan
		log.Infof("feeder %s stopped", k)
	}
	close(stopChan)
}
