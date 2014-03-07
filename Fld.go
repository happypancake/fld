package fld

import (
	"fmt"
	"net"

	stdetcd "github.com/coreos/go-etcd/etcd"
	"github.com/happypancake/go-integrate/etcd"
	"github.com/op/go-logging"
)

var (
	address   string
	outgoing  = make(chan []byte, 100000)
	conn      net.Conn
	addresses = make(chan string)
	log       = logging.MustGetLogger("fld")
)

type LoggingBackend struct{}

func NewLoggingBackend() *LoggingBackend {
	return &LoggingBackend{}
}

func (*LoggingBackend) Log(level logging.Level, depth int, rec *logging.Record) error {

	switch level {
	case logging.CRITICAL:
		return send("crit", rec.Formatted())
	case logging.ERROR:
		return send("err", rec.Formatted())
	case logging.WARNING:
		return send("warning", rec.Formatted())
	case logging.NOTICE:
		return send("notice", rec.Formatted())
	case logging.INFO:
		return send("info", rec.Formatted())
	case logging.DEBUG:
		return send("debug", rec.Formatted())
	default:
	}
	panic("unhandled log level")
}

func InitWithDynamicConfig(client *stdetcd.Client, hostname string) {
	key := fmt.Sprintf("/%v/logsd/address", hostname)

	log.Info("Configuring fld by watching key: %v", key)
	go etcd.GetAndWatchStringValue(client, key, addresses, nil)
	go processOutgoing()
}

func send(severity string, name string) error {

	length := float64(len(outgoing))
	capacity := float64(cap(outgoing))

	if length < capacity*0.9 {
		payload := severity + "|" + name
		outgoing <- []byte(payload)
	}
	return nil
}

func connect() error {

	c, err := net.Dial("udp", address)
	if err != nil {
		return err
	}

	conn = c
	return nil
}

func hasAddress() bool {
	return address != ""
}

func processOutgoing() {
	for {
		select {
		case outgoing := <-outgoing:
			if !hasAddress() {
				break
			}

			if _, err := conn.Write(outgoing); err != nil {
				connect()
			}
		case address = <-addresses:
			log.Info("logsd address received: %v", address)
			connect()
		}
	}
}
