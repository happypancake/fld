package fld

import (
	"flag"
	"net"

	"github.com/op/go-logging"
)

var (
	Instance *Fld
	address  *string = flag.String("logsd", "127.0.0.1:8127", "UDP endpoint for LogsD daemon")
)

type Fld struct {
	outgoing chan []byte
	conn     net.Conn
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
	start()
}

func start() {
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

		if nil == fld.conn {
			fld.connect()
		}

		if _, err := fld.conn.Write(outgoing); err != nil {
			fld.connect()
		}
	}
}
