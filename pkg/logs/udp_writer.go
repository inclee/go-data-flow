package logs

import (
	"fmt"
	"net"
)

type UDPWriter struct {
	conn *net.UDPConn
}

func NewUDPWriter(addr string) (*UDPWriter, error) {
	conn, err := net.Dial("udp", addr)
	if nil != err {
		return nil, err
	}
	return &UDPWriter{conn: conn.(*net.UDPConn)}, nil
}

func (w *UDPWriter) Close() {
	w.conn.Close()
	w.conn = nil
}

func (w *UDPWriter) Flush() {
}

func (w *UDPWriter) WriteString(s string) (n int, err error) {
	if w.conn != nil {
		return w.conn.Write([]byte(fmt.Sprintf("%v\n", s)))
	}
	return 0, nil
}

func (w *UDPWriter) Write(p []byte) (n int, err error) {
	if w.conn != nil {
		return w.conn.Write(p)
	}
	return 0, nil
}
