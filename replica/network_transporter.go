package replica

import (
	"bufio"
	"encoding/xml"
	"log"
	"math/rand"
	"net"
)

type NetworkTransporter struct {
	Addrs      []*net.UDPAddr
	Self       uint8
	FastQuorum uint8
	All        uint8
	Conns      []*net.UDPConn
}

func NewNetworkTransporter(addrStrs []string,
	self uint8, size uint8) (*NetworkTransporter, error) {

	var err error
	addrs := make([]*net.UDPAddr, len(addrStrs))
	conns := make([]*net.UDPConn, len(addrStrs))

	for i := range addrs {
		if uint8(i) == self {
			continue
		}

		addrs[i], err = net.ResolveUDPAddr("udp", addrStrs[i])
		if err != nil {
			return nil, err
		}

		conns[i], err = net.DialUDP("udp", nil, addrs[i])
		if err != nil {
			return nil, err
		}
	}

	nt := &NetworkTransporter{
		Addrs:      addrs,
		Self:       self,
		FastQuorum: size - 2,
		All:        size - 1,
		Conns:      conns,
	}
	return nt, nil
}

func (nt *NetworkTransporter) Send(to uint8, msg Message) {
	go func() {
		conn := nt.Conns[to]
		if conn == nil {
			log.Println("shouldn't sent to ", to)
			return
		}

		b, err := xml.Marshal(msg)
		if err != nil {
			log.Println("Marshal:", err)
			return
		}

		bw := bufio.NewWriter(conn)
		bw.WriteByte(nt.Self)
		bw.WriteByte(msg.Type())
		bw.Write(b)
		bw.Flush()
	}()
}

func (nt *NetworkTransporter) MulticastFastquorum(msg Message) {
	skip := uint8(rand.Intn(int(nt.All)))
	if skip == nt.Self {
		skip = nt.All - 1
	}

	for i := uint8(0); i < nt.All; i++ {
		if i == nt.Self || i == skip {
			continue
		}
		nt.Send(i, msg)
	}
}

func (nt *NetworkTransporter) Broadcast(msg Message) {
	for i := uint8(0); i < nt.All; i++ {
		if i == nt.Self {
			continue
		}
		nt.Send(i, msg)
	}
}
