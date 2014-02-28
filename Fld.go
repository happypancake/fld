package fld

import (
	"net"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/op/go-logging"
)

var (
	Instance      *Fld
	address       string
	outgoing      chan []byte
	conn          net.Conn
	addressConfig chan *etcd.Response
	log           = logging.MustGetLogger("fsd")
)

type Fld struct{}

func (fld *Fld) Log(level logging.Level, depth int, rec *logging.Record) error {

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

func InitWithDynamicConfig(client *etcd.Client) {
	addressConfig = make(chan *etcd.Response)

	go watchConfiguration(client, "/logsd/address")
	go processOutgoing()
}

func watchConfiguration(client *etcd.Client, key string) {
	for {
		if _, err := client.Watch(key, 0, false, addressConfig, nil); err != nil {
			toSleep := 5 * time.Second

			log.Debug("error watching etcd for key %v: %v", key, err)
			log.Debug("retry in %v", toSleep)
			time.Sleep(toSleep)
		}
	}
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
		case response := <-addressConfig:
			if response.Node != nil && response.Node.Value != "" {
				address = response.Node.Value
				connect()
			}
		}
	}
}
