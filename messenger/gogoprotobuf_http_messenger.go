package messenger

import (
	"github.com/go-distributed/epaxos"
	"github.com/go-distributed/epaxos/codec"
	"github.com/go-distributed/epaxos/transporter"
)

// Create a new messenger that uses GoGoprotobuf over HTTP.
func NewGoGoProtobufHTTPMessenger(hostports map[uint8]string, self uint8, size int, chanSize int) (*epaxos.EpaxosMessenger, error) {
	m := &epaxos.EpaxosMessenger{
		Hostports:  hostports,
		Self:       self,
		FastQuorum: size - 1,
		All:        size,
	}

	tr, err := transporter.NewHTTPTransporter(hostports[self], chanSize)
	if err != nil {
		return nil, err
	}

	codec, err := codec.NewGoGoProtobufCodec()
	if err != nil {
		return nil, err
		// This can't happen, added here
		// for symmetric looking.
	}

	m.Tr, m.Codec = tr, codec
	return m, nil
}
