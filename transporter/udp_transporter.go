package transporter

import (
	"encoding/gob"
	"math/rand"
	"net"

	"github.com/go-distributed/epaxos/message"
	"github.com/golang/glog"
)

type UDPTransporter struct {
	Addrs      []*net.UDPAddr
	Self       uint8
	FastQuorum uint8
	All        uint8
	Conns      []*net.UDPConn
	ch         chan message.Message
	stop       chan struct{}
	encs       []*gob.Encoder
	dec        *gob.Decoder
}

func NewUDPTransporter(addrStrs []string,
	self uint8, size int) (*UDPTransporter, error) {

	var err error
	addrs := make([]*net.UDPAddr, size)
	conns := make([]*net.UDPConn, size)

	gob.Register(&message.Propose{})
	gob.Register(&message.PreAccept{})
	gob.Register(&message.PreAcceptOk{})
	gob.Register(&message.PreAcceptReply{})
	gob.Register(&message.Accept{})
	gob.Register(&message.AcceptReply{})
	gob.Register(&message.Commit{})
	gob.Register(&message.Prepare{})
	gob.Register(&message.PrepareReply{})

	var dec *gob.Decoder
	for i := range addrs {
		addrs[i], err = net.ResolveUDPAddr("udp", addrStrs[i])
		if err != nil {
			return nil, err
		}

		// get incoming connection
		if uint8(i) == self {
			conns[i], err = net.ListenUDP("udp", addrs[i])
			if err != nil {
				return nil, err
			}
			dec = gob.NewDecoder(conns[i])
		}
	}

	nt := &UDPTransporter{
		Addrs:      addrs,
		Self:       self,
		FastQuorum: uint8(size) - 2,
		All:        uint8(size),
		Conns:      conns,
		stop:       make(chan struct{}),
		encs:       make([]*gob.Encoder, size),
		dec:        dec,
	}
	return nt, nil
}

func (nt *UDPTransporter) Send(to uint8, msg message.Message) {
	go func() {
		err := nt.encs[to].Encode(&msg)
		if err != nil {
			glog.Warning("Encoding error ", err)
		}
	}()
}

func (nt *UDPTransporter) MulticastFastquorum(msg message.Message) {
	skip := uint8(rand.Intn(int(nt.All)))
	if skip == nt.Self {
		skip = (skip + 1) % nt.All
	}

	for i := uint8(0); i < nt.All; i++ {
		if i == nt.Self || i == skip {
			continue
		}
		nt.Send(i, msg)
	}
}

func (nt *UDPTransporter) Broadcast(msg message.Message) {
	for i := uint8(0); i < nt.All; i++ {
		if i == nt.Self {
			continue
		}
		nt.Send(i, msg)
	}
}

func (nt *UDPTransporter) RegisterChannel(ch chan message.Message) {
	nt.ch = ch
}

func (nt *UDPTransporter) Start() error {
	// get outgoint connection
	var err error

	for i := range nt.Conns {
		if i == int(nt.Self) {
			continue
		}
		nt.Conns[i], err = net.DialUDP("udp", nil, nt.Addrs[i])
		if err != nil {
			return err
		}
		nt.encs[i] = gob.NewEncoder(nt.Conns[i])
	}

	// start receive loop
	var msg message.Message
	go func() {
		for {
			select {
			case <-nt.stop:
				return
			default:
			}

			// receive message
			err := nt.dec.Decode(&msg)
			if err != nil {
				glog.Warning("Decoding error ", err)
				continue
			}
			nt.ch <- msg
		}
	}()
	return nil
}

func (nt *UDPTransporter) Stop() {
	close(nt.stop)
	// stop network
	for _, conn := range nt.Conns {
		conn.Close()
	}
}
