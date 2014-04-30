package transporter

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"net"

	"github.com/go-distributed/epaxos/message"
	"github.com/golang/glog"
)

const defaultUDPSize = 8192

type UDPTransporter struct {
	Addrs      []*net.UDPAddr
	Self       uint8
	FastQuorum uint8
	All        uint8
	Conns      []*net.UDPConn
	ch         chan message.Message
	stop       chan struct{}

	encBuffer *bytes.Buffer //TODO: race condition
	decBuffer *bytes.Buffer
	enc       *gob.Encoder
	dec       *gob.Decoder
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
		}
	}

	nt := &UDPTransporter{
		Addrs:      addrs,
		Self:       self,
		FastQuorum: uint8(size) - 2,
		All:        uint8(size),
		Conns:      conns,
		stop:       make(chan struct{}),
		encBuffer:  new(bytes.Buffer),
		decBuffer:  new(bytes.Buffer),
	}
	nt.enc = gob.NewEncoder(nt.encBuffer)
	nt.dec = gob.NewDecoder(nt.decBuffer)

	return nt, nil
}

func (nt *UDPTransporter) Send(to uint8, msg message.Message) {
	go func() {
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(&msg); err != nil {
			glog.Warning("Encoding error ", err)
		}
		_, err := nt.Conns[to].Write(buf.Bytes())
		if err != nil {
			glog.Warning("UDP write error ", err)
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
	}

	// start receive loop
	var msg message.Message
	b := make([]byte, defaultUDPSize)
	buf := new(bytes.Buffer)
	go func() {
		for {
			select {
			case <-nt.stop:
				return
			default:
			}

			// receive message
			n, _, err := nt.Conns[nt.Self].ReadFrom(b)
			if err != nil {
				glog.Warning("UDP read error ", err)
				continue
			}

			buf.Reset()
			buf.Write(b[:n])
			dec := gob.NewDecoder(buf)
			err = dec.Decode(&msg)
			if err != nil {
				glog.Warning("Decoding error ", err)
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
