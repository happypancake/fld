package fld

import (
	"fmt"
	"net"
)

var (
	Instance *Fld
)

type Fld struct {
	outgoing chan string
	address  string
	conn     net.Conn
}

func init() {
	Start("127.0.0.1:8127")
}

func Start(address string) {
	Instance = &Fld{address: address, outgoing: make(chan string, 100000)}
	Instance.connect()

	go Instance.processOutgoing()
}

func (fld *Fld) connect() error {
	conn, err := net.Dial("udp", fld.address)
	if err != nil {
		return err
	}

	fld.conn = conn
	return nil
}

func (fld *Fld) processOutgoing() {
	for outgoing := range fld.outgoing {
		if _, err := fld.conn.Write([]byte(outgoing)); err != nil {
			fld.connect()
		}
	}
}

func Info(entry string) {
	payload := createPayload(entry, "info")
	send(payload)
}

func Debug(entry string) {
	payload := createPayload(entry, "debug")
	send(payload)
}
func Warning(entry string) {
	payload := createPayload(entry, "warning")
	send(payload)
}
func Error(entry string) {
	payload := createPayload(entry, "error")
	send(payload)
}
func createPayload(name string, severity string) string {
	return fmt.Sprintf("%s|%s", name, severity)
}

func send(payload string) {
	length := float64(len(Instance.outgoing))
	capacity := float64(cap(Instance.outgoing))

	if length < capacity*0.9 {
		Instance.outgoing <- payload
	}
}
