package epaxos

import (
	"math/rand"

	"github.com/go-distributed/epaxos/message"
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
	MulticastFastQuorum(msg message.Message) error

	// Broadcast a message to all peers.
	Broadcast(msg message.Message) error

	// Tries to receive a message.
	Recv() (message.Message, error)

	// Start the messenger.
	Start() error

	// Stop the messenger.
	Stop() error

	// Destroy the messenger.
	Destroy() error
}

type EpaxosMessenger struct {
	Hostports  map[uint8]string
	Self       uint8
	FastQuorum int // Consider to remove this.
	All        int
	Tr         Transporter
	Codec      Codec
}

// Send a message to the specified peer.
func (m *EpaxosMessenger) Send(to uint8, msg message.Message) error {
	data, err := m.Codec.Marshal(msg)
	if err != nil {
		return err
	}
	return m.Tr.Send(m.Hostports[to], msg.Type(), data)
}

// Multicast a message to the fast quorum.
func (m *EpaxosMessenger) MulticastFastQuorum(msg message.Message) error {
	data, err := m.Codec.Marshal(msg)
	if err != nil {
		return err
	}

	skip := uint8(rand.Intn(m.All))
	if skip == m.Self {
		skip = (skip + 1) % uint8(m.All)
	}

	for i := uint8(0); i < uint8(m.All); i++ {
		if i == m.Self || i == skip {
			// Skip itSelf and one more.
			continue
		}
		err = m.Tr.Send(m.Hostports[i], msg.Type(), data)
		if err != nil {
			return err
		}
	}
	return nil
}

// Broadcast a message to all peers.
func (m *EpaxosMessenger) Broadcast(msg message.Message) error {
	data, err := m.Codec.Marshal(msg)
	if err != nil {
		return err
	}

	for i := uint8(0); i < uint8(m.All); i++ {
		if i == m.Self { // Skip itself.
			continue
		}
		err = m.Tr.Send(m.Hostports[i], msg.Type(), data)
		if err != nil {
			return err
		}
	}
	return nil
}

// Tries to receive a message.
func (m *EpaxosMessenger) Recv() (message.Message, error) {
	mtype, data, err := m.Tr.Recv()
	if err != nil {
		return nil, err
	}
	return m.Codec.Unmarshal(mtype, data)
}

// Start the messenger.
func (m *EpaxosMessenger) Start() error {
	if err := m.Codec.Initial(); err != nil {
		return err
	}
	return m.Tr.Start()
}

// Stop the messenger.
func (m *EpaxosMessenger) Stop() error {
	if err := m.Tr.Stop(); err != nil {
		return err
	}
	return m.Codec.Stop()
}

// Destroy the messenger.
func (m *EpaxosMessenger) Destroy() error {
	if err := m.Tr.Destroy(); err != nil {
		return err
	}
	return m.Codec.Destroy()
}
