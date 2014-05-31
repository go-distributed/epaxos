package epaxos

import (
	"math/rand"

	"github.com/go-distributed/epaxos/codec"
	"github.com/go-distributed/epaxos/message"
	"github.com/go-distributed/epaxos/transporter"
)

// A messenger can:
// 1, Send a message to the specified peer.
// 2, Multicast a message to a fast quorum of peers.
// 3, Broad a message to all peers.
// 4, Receive a message.
// All operations will block.
type Messenger interface {
	// Send a message to a specified peer.
	Send(to uint8, msg message.Message) error

	// Multicast a message to the fast quorum.
	MulticastFastquorum(msg message.Message) error

	// Broadcast a message to all peers.
	Broadcast(msg message.Message) error

	// Tries to receive a message.
	Recv() (message.Message, error)

	// Start the messenger.
	Start() error

	// Stop the messenger.
	Stop() error
}

type EpaxosMessenger struct {
	hostports  map[uint8]string
	self       uint8
	fastQuorum int // Consider to remove this.
	all        int
	tr         Transporter
	codec      Codec
}

// Send a message to the specified peer.
func (m *EpaxosMessenger) Send(to uint8, msg message.Message) error {
	data, err := m.codec.Marshal(msg)
	if err != nil {
		return err
	}
	return m.tr.Send(m.hostports[to], msg.Type(), data)
}

// Multicast a message to the fast quorum.
func (m *EpaxosMessenger) MulticastFastQuorum(msg message.Message) error {
	data, err := m.codec.Marshal(msg)
	if err != nil {
		return err
	}

	skip := uint8(rand.Intn(m.all))
	if skip == m.self {
		skip = (skip + 1) % uint8(m.all)
	}

	for i := uint8(0); i < uint8(m.all); i++ {
		if i == m.self || i == skip {
			// Skip itself and one more.
			continue
		}
		err = m.tr.Send(m.hostports[i], msg.Type(), data)
		if err != nil {
			return err
		}
	}
	return nil
}

// Broadcast a message to all peers.
func (m *EpaxosMessenger) Broadcast(msg message.Message) error {
	data, err := m.codec.Marshal(msg)
	if err != nil {
		return err
	}

	for i := uint8(0); i < uint8(m.all); i++ {
		if i == m.self { // Skip itself.
			continue
		}
		err = m.tr.Send(m.hostports[i], msg.Type(), data)
		if err != nil {
			return err
		}
	}
	return nil
}

// Tries to receive a message.
func (m *EpaxosMessenger) Recv() (message.Message, error) {
	mtype, data, err := m.tr.Recv()
	if err != nil {
		return nil, err
	}
	return m.codec.Unmarshal(mtype, data)
}

// Start the messenger.
func (m *EpaxosMessenger) Start() error {
	if err := m.codec.Initial(); err != nil {
		return err
	}
	return m.tr.Start()
}

// Stop the messenger.
func (m *EpaxosMessenger) Stop() error {
	if err := m.tr.Stop(); err != nil {
		return err
	}
	return m.codec.Destroy()
}

// Create a new messenger that uses GoGoprotobuf over HTTP.
func NewGoGoProtobufHTTPMessenger(hostports map[uint8]string, self uint8, size int) (*EpaxosMessenger, error) {
	m := &EpaxosMessenger{
		hostports:  hostports,
		self:       self,
		fastQuorum: size - 1,
		all:        size,
	}

	tr, err := transporter.NewHTTPTransporter(hostports[self])
	if err != nil {
		return nil, err
	}

	codec, err := codec.NewGoGoProtobufHTTPTransporter()
	if err != nil {
		return nil, err
		// This can't happen, added here
		// for symmetric looking.
	}

	m.tr, m.codec = tr, codec
	return m, nil
}
