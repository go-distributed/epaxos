package replica

import (
	"encoding/json"
	"math/rand"
	"net"

	"github.com/golang/glog"
)

type NetworkTransporter struct {
	Addrs      []*net.UDPAddr
	Self       uint8
	FastQuorum uint8
	All        uint8
}

func NewNetworkTransporter(addrStrs []string,
	self uint8, size uint8) (*NetworkTransporter, error) {

	var err error
	addrs := make([]*net.UDPAddr, len(addrStrs))

	for i := range addrs {
		addrs[i], err = net.ResolveUDPAddr("udp", addrStrs[i])
		if err != nil {
			return nil, err
		}
	}

	nt := &NetworkTransporter{
		Addrs:      addrs,
		Self:       self,
		FastQuorum: size - 2,
		All:        size - 1,
	}
	return nt, nil
}

func (nt *NetworkTransporter) Send(to uint8, msg Message) {
	go func() {
		addr := nt.Addrs[to]
		msgEvent := &MessageEvent{nt.Self, msg}
		data, err := json.Marshal(msgEvent)
		if err != nil {
			glog.Errorln("Marshal:", err)
			return
		}

		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			glog.Errorln("Transporter send:", err)
			return
		}

		conn.Write(data)
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

func (nt *NetworkTransporter) Broadcast(to uint8, msg Message) {
	for i := uint8(0); i < nt.All; i++ {
		if i == nt.Self {
			continue
		}
		nt.Send(i, msg)
	}
}
