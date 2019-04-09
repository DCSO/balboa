// balboa-backends
// Copyright (c) 2019, DCSO GmbH

package backend

import (
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/DCSO/balboa/db"

	log "github.com/sirupsen/logrus"
)

type Handler interface {
	HandleObservations(*db.InputRequest)
	HandleQuery(*db.QueryRequest, net.Conn)
	HandleDump(*db.DumpRequest, net.Conn)
	HandleBackup(*db.BackupRequest)
}

var (
	DecodeTimeout      = 10 * time.Second
	QueryResultTimeout = 10 * time.Second
	AcceptTimeout      = 10 * time.Second
)

func handle(conn net.Conn, h Handler, stopCh chan bool, wg *sync.WaitGroup) {
	dec := db.MakeDecoder(conn)
	defer dec.Release()
	defer conn.Close()
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			log.Debugf("stop signal received")
			return
		default:
			msg, err := dec.ExpectTypedMessage()
			if err != nil {
				log.Warnf("unable to decode typed message `%v`", err)
				return
			}
			switch msg.Type {
			case db.TypeInputRequest:
				log.Debugf("got input message")
				inner, err_inner := dec.ExpectInputRequestFromBytes(msg.EncodedMessage)
				if err_inner != nil {
					log.Warnf("unable to decode inner message: input request")
					return
				}
				h.HandleObservations(inner)
			case db.TypeQueryRequest:
				log.Debugf("got query message")
				inner, err_inner := dec.ExpectQueryRequestFromBytes(msg.EncodedMessage)
				if err_inner != nil {
					log.Warnf("unable to decode inner message: query request")
					return
				}
				h.HandleQuery(inner,conn)
			case db.TypeBackupRequest:
				log.Debugf("got backup request")
				inner, err_inner := dec.ExpectBackupRequestFromBytes(msg.EncodedMessage)
				if err_inner != nil {
					log.Warnf("unable to decode inner message: backup request")
					return
				}
				h.HandleBackup(inner)
			case db.TypeDumpRequest:
				log.Debugf("got dump request")
				inner, err_inner := dec.ExpectDumpRequestFromBytes(msg.EncodedMessage)
				if err_inner != nil {
					log.Warnf("unable to decode inner message: dump request")
					return
				}
				h.HandleDump(inner, conn)
			default:
				log.Warnf("invalid or unsupported message type `%v`", msg.Type)
				return
			}
		}
	}
}

func loop(host string, h Handler, stopCh chan bool, wg *sync.WaitGroup) {
	log.Infof("start listening on host=%s", host)
	ln, err := net.Listen("tcp", host)
	if err != nil {
		log.Warnf("unable to create listening socket: %s", err)
		return
	}
	defer ln.Close()
	defer wg.Done()
	for {
		select {
		case <-stopCh:
			return
		default:
			ln.(*net.TCPListener).SetDeadline(time.Now().Add(AcceptTimeout))
			conn, err := ln.Accept()
			if err != nil {
				if operr, ok := err.(*net.OpError); ok && operr.Timeout() {
					log.Debugf("accepting client connection timeout")
					continue
				}
				log.Warnf("unable to accept new client connection `%s`", err)
				continue
			}
			log.Debugf("handling new connection")
			wg.Add(1)
			go handle(conn, h, stopCh, wg)
		}
	}
}

func Serve(host string, h Handler) {
	sigCh := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Warnf("received '%v' signal, shutting down ...", sig)
		close(done)
	}()

	var wg sync.WaitGroup

	wg.Add(1)

	go loop(host, h, done, &wg)

	wg.Wait()
}
