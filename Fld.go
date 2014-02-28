package fld

import (
	"net"
	"time"

	"github.com/op/go-logging"
)

var (
	Instance *Fld
	address  *string
)

type Fld struct {
	outgoing chan []byte
	conn     net.Conn
}

// SendTo switches from the current address
// to specified one
func SendTo(newAddress string) {
	address = &newAddress
}

func (fld *Fld) Log(level logging.Level, depth int, rec *logging.Record) error {

	switch level {
	case logging.CRITICAL:
		return send(fld, "crit", rec.Formatted())
	case logging.ERROR:
		return send(fld, "err", rec.Formatted())
	case logging.WARNING:
		return send(fld, "warning", rec.Formatted())
	case logging.NOTICE:
		return send(fld, "notice", rec.Formatted())
	case logging.INFO:
		return send(fld, "info", rec.Formatted())
	case logging.DEBUG:
		return send(fld, "debug", rec.Formatted())
	default:
	}
	panic("unhandled log level")
}

func init() {
	*address = "127.0.0.1:8127"
	Instance = &Fld{outgoing: make(chan []byte, 100000)}
	go Instance.processOutgoing()
}

func send(fld *Fld, severity string, name string) error {
	length := float64(len(Instance.outgoing))
	capacity := float64(cap(Instance.outgoing))

	if length < capacity*0.9 {
		payload := severity + "|" + name
		Instance.outgoing <- []byte(payload)
	}
	return nil
}

func (fld *Fld) connect() error {
	conn, err := net.Dial("udp", *address)
	if err != nil {
		return err
	}

	fld.conn = conn
	return nil
}

func (fld *Fld) processOutgoing() {
	for outgoing := range fld.outgoing {

		// try reconnecting till success
		for nil == fld.conn {
			if err := fld.connect(); err != nil {
				// if we failed to connect, sleep
				time.Sleep(time.Second)
			}
		}

		if _, err := fld.conn.Write(outgoing); err != nil {
			fld.connect()
		}
	}
}
