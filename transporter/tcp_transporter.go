package transporter

import (
	//"encoding/gob"
	//"math/rand"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/go-distributed/epaxos/message"
	"github.com/golang/glog"
)

var errCannotEstablishConnetions = errors.New("tcp_transporter: cannot establish connections")

const defaultRetryDuration = 50 * time.Millisecond
const defaultRetrial = 5

type TCPTransporter struct {
	ids        []uint8
	tcpAddrs   map[uint8]*net.TCPAddr
	self       uint8
	fastQuorum int
	all        int

	ln       *net.TCPListener
	inConns  map[uint8]*net.TCPConn
	outConns map[uint8]*net.TCPConn

	replicaCh chan message.Message
	stop      chan struct{}
}

func NewTCPTransporter(addrStrs []string, self uint8, size int) (*TCPTransporter, error) {
	tt := &TCPTransporter{
		ids:        make([]uint8, size),
		tcpAddrs:   make(map[uint8]*net.TCPAddr),
		self:       self,
		fastQuorum: size - 2,
		all:        size,

		inConns:  make(map[uint8]*net.TCPConn),
		outConns: make(map[uint8]*net.TCPConn),

		replicaCh: make(chan message.Message), // TODO: more buffer size
		stop:      make(chan struct{}),
	}

	for i := 0; i < size; i++ {
		tt.ids[i] = uint8(i)
	}

	// resolve tcp addrs
	var err error
	for i := range addrStrs {
		id := tt.ids[i]
		addrStr := addrStrs[i]

		tt.tcpAddrs[id], err = net.ResolveTCPAddr("tcp", addrStr)
		if err != nil {
			glog.Warning("ResolveTCPAddr error: ", err)
			return nil, err
		}
	}
	return tt, nil
}

func (tt *TCPTransporter) RegisterChannel(ch chan message.Message) {
	tt.replicaCh = ch
}

func (tt *TCPTransporter) Send(to uint8, msg message.Message) {
	go func() {
		// TODO: marshal
		conn := tt.outConns[to]
		_, err := conn.Write([]byte("something"))
		if err != nil {
			// TODO: connection lost error?
			remoteAddr := conn.RemoteAddr()
			glog.Warning("Write error from connection: ", remoteAddr, err)

			// try to reconnect
			tt.dial(to)
		}
	}()
}

func (tt *TCPTransporter) MulticastFastquorum(msg message.Message) {
}

func (tt *TCPTransporter) Broadcast(msg message.Message) {
}

// try to estable a tcp connetion with a remote address,
// and then save the connection in the map
func (tt *TCPTransporter) dial(id uint8) error {
	addr := tt.tcpAddrs[id]
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		glog.Warning("Dial error: ", err)
		return err
	}
	tt.outConns[id] = conn
	return nil
}

// try to establish outgoing connetions with other peers
func (tt *TCPTransporter) dialLoop() error {
	success := false

	for i := 0; i < defaultRetrial; i++ {
		glog.Infof("Trial No.%d\n", i+1)
		for _, id := range tt.ids {
			if id == tt.self {
				continue
			}
			if tt.outConns[id] != nil { // skip already established connections
				continue
			}
			if err := tt.dial(id); err != nil {
				continue
			}
		}

		// test if all outgoing connections are established
		if len(tt.outConns) >= tt.all-1 {
			success = true
			break
		}
		glog.Infof("Will retry after %d seconds\n", defaultRetryDuration/1000000000)
		time.Sleep(defaultRetryDuration)
	}

	if !success {
		return errCannotEstablishConnetions
	}
	return nil
}

func (tt *TCPTransporter) findIDByAddr(addr net.Addr) uint8 {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		panic("SplitHostPort error, which is impossible here")
	}

	// just iterate to find the id is ok since the quorum is small
	// and findIDByAddr() is rarely called
	for id, addr := range tt.tcpAddrs {
		ihost, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			panic("SplitHostPort error, which is impossible here")
		}
		if host == ihost { // found
			return id
		}
	}
	glog.Warning("invalid remote addr: ", addr)
	panic("")
}

// start listen and then keeping accepting new connections,
// once a new connection is established, start a goroutine to read it
func (tt *TCPTransporter) run() {
	var err error

	tt.ln, err = net.ListenTCP("tcp", tt.tcpAddrs[tt.self])
	if err != nil {
		glog.Warning("Listen error: ", err)
		return
	}

	// keep accepting connections
	for {
		select {
		case <-tt.stop:
			return
		default:
		}

		conn, err := tt.ln.AcceptTCP()
		if err != nil {
			glog.Warning("Accept error: ", err)
			continue
		}

		id := tt.findIDByAddr(conn.RemoteAddr())
		tt.inConns[id] = conn
		go tt.readLoop(conn)
	}
}

// keep reading from one incoming connection,
// if the connection gets lost, then return
func (tt *TCPTransporter) readLoop(conn *net.TCPConn) {
	for {
		select {
		case <-tt.stop:
			return
		default:
		}

		for {
			var b []byte
			_, err := conn.Read(b)
			if err != nil {
				// TODO: to detect connection lost error
				remoteAddr := conn.RemoteAddr()
				glog.Warning("Read error from connection: ", remoteAddr, err)
				return
			}

			// TODO: unmarshal and send to channel
			fmt.Println(b)
		}
	}
}

func (tt *TCPTransporter) Start() error {
	// start dial loop, wait for all outgoing connections
	if err := tt.dialLoop(); err != nil {
		return err
	}

	// start accept and read loop
	go tt.run()
	return nil
}

func (tt *TCPTransporter) Stop() {
}
