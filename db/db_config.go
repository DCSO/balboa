// balboa
// Copyright (c) 2018, 2019, DCSO GmbH

package db

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

// Setup represents a database backend configuration, typically provided via
// YAML.
type Setup struct {
	Database struct {
		Name string `yaml:"name"`
		Type string `yaml:"type"`
		HostAndPort string `yaml:"host"`
		// for local storage
		DBPath string `yaml:"db_path"`
		// for RocksDB
		MemtableMemBudget uint64 `yaml:"membudget"`
		// for Cassandra
		Hosts    []string `yaml:"hosts"`
		Username string   `yaml:"username"`
		Password string   `yaml:"password"`
		Nworkers uint     `yaml:"nof_workers"`
	} `yaml:"database"`
	LoadedDB DB
}

// LoadSetup parses a given YAML description into a new Setup structure.
func LoadSetup(in []byte) (*Setup, error) {
	var s Setup
	err := yaml.Unmarshal(in, &s)
	if err != nil {
		return nil, err
	}
	if s.Database.Name == "" {
		return nil, fmt.Errorf("database name missing")
	}
	if s.Database.Type == "" {
		return nil, fmt.Errorf("database type missing")
	}
	switch s.Database.Type {
	case "remote-backend":
		if len(s.Database.HostAndPort) == 0 {
			return nil, fmt.Errorf("%s: no host defined",s.Database.Name)
		}
	}
	return &s, nil
}

// Run creates the necessary database objects and makes them ready to
// consume or provide data.
func (s *Setup) Run() (DB, error) {
	log.Infof("starting database %s", s.Database.Name)
	var db DB
	var err error
	switch s.Database.Type {
	case "remote-backend":
		db,err=MakeRemoteBackend(s.Database.HostAndPort,true)
		if err!=nil {
			return nil,err
		}
	}
	s.LoadedDB = db
	return db, nil
}

// Stop causes a given Setup to shut down the corresponding database
// connections defined within.
func (s *Setup) Stop(stopChan chan bool) {
	s.LoadedDB.Shutdown()
	close(stopChan)
}
