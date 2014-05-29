package transporter

import (
	//"encoding/gob"
	//"math/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-distributed/epaxos/message"
	"github.com/golang/glog"
)

var errCannotEstablishConnetions = errors.New("tcp_transporter: cannot establish connections")

const defaultRetryDuration = 50 * time.Millisecond
const defaultRetrial = 5
const defaultReconnectChanSize = 1024

type TCPTransporter struct {
	ids        []uint8
	tcpAddrs   map[uint8]*net.TCPAddr
	self       uint8
	fastQuorum int
	all        int

	ln            *net.TCPListener
	inConns       map[uint8]*net.TCPConn
	outConns      map[uint8]*net.TCPConn
	outConnStates map[uint8]bool
	outConnRW     sync.RWMutex

	reconnectCh chan uint8
	replicaCh   chan message.Message
	stop        chan struct{}

	//codec codec.Codec
}

func NewTCPTransporter(addrStrs []string, self uint8, size int) (*TCPTransporter, error) {
	tt := &TCPTransporter{
		ids:        make([]uint8, size),
		tcpAddrs:   make(map[uint8]*net.TCPAddr),
		self:       self,
		fastQuorum: size - 2,
		all:        size,

		inConns:       make(map[uint8]*net.TCPConn),
		outConns:      make(map[uint8]*net.TCPConn),
		outConnStates: make(map[uint8]bool),

		reconnectCh: make(chan uint8, defaultReconnectChanSize),
		replicaCh:   make(chan message.Message), // TODO: more buffer size.
		stop:        make(chan struct{}),
	}
	for i := 0; i < size; i++ {
		tt.ids[i] = uint8(i)
	}

	// Resolve tcp addresses.
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
		// TODO: marshal.
		conn := tt.outConns[to]
		_, err := conn.Write([]byte("something"))
		if err != nil {
			// TODO: connection lost error?
			remoteAddr := conn.RemoteAddr()
			glog.Warning("Write error from connection: ",
				remoteAddr, err)

			tt.outConnRW.Lock()
			defer tt.outConnRW.Unlock()

			// Close the connection and invalidate its state.
			tt.outConns[to].Close()
			tt.outConnStates[to] = false
			tt.reconnectCh <- to
		}
	}()
}

func (tt *TCPTransporter) MulticastFastquorum(msg message.Message) {
}

func (tt *TCPTransporter) Broadcast(msg message.Message) {
}

// Try to estable a tcp connetion with a remote address,
// and then save the connection in the map.
func (tt *TCPTransporter) dial(id uint8) error {
	tt.outConnRW.Lock()
	defer tt.outConnRW.Unlock()

	// Return immediately if the connection is valid, which means
	// that the connection is probably reconstructed successfully
	// some time before.
	if tt.outConnStates[id] == true {
		return nil
	}
	// Otherwise, try to reconnect.
	addr := tt.tcpAddrs[id]
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		glog.Warning("Dial error: ", err)
		return err
	}
	tt.outConns[id] = conn
	tt.outConnStates[id] = true
	return nil
}

// Try to establish outgoing connetions with other peers.
func (tt *TCPTransporter) dialLoop() error {
	success := false

	for i := 0; i < defaultRetrial; i++ {
		glog.Infof("Trial No.%d\n", i+1)
		for _, id := range tt.ids {
			if id == tt.self {
				continue
			}
			// Skip already established connections.
			if tt.outConns[id] != nil {
				continue
			}
			if err := tt.dial(id); err != nil {
				glog.Info("Dial error ", err)
				continue
			}
		}
		// Test if all outgoing connections are established.
		if len(tt.outConns) >= tt.all-1 {
			success = true
			break
		}
		glog.Warning("Will retry after ", defaultRetryDuration.String())
		time.Sleep(defaultRetryDuration)
	}

	if !success {
		return errCannotEstablishConnetions
	}
	return nil
}

// Will monitor and try to reconnect any disconnected outgoing connections.
func (tt *TCPTransporter) outgoingLoop() {
	for {
		select {
		case <-tt.stop:
			return
		case to := <-tt.reconnectCh:
			tt.dial(to)
		}
	}
}

// Return false if not found, otherwise return true with
// the replica-id of the incoming connection.
func (tt *TCPTransporter) findIDByAddr(addr net.Addr) (uint8, bool) {
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		panic("SplitHostPort error, which is impossible here")
	}

	// Just iterate to find the id is ok since the quorum is small
	// and findIDByAddr() is rarely called.
	for id, addr := range tt.tcpAddrs {
		ihost, iport, err := net.SplitHostPort(addr.String())
		if err != nil {
			panic("SplitHostPort error, which is impossible here")
		}

		if host == ihost && iport == port { // Found
			return id, true
		}
	}

	glog.Warning("invalid remote addr: ", addr)
	return 0, false
}

// Start listen and then keep accepting new connections,
// for each newly established connection, start a goroutine to read it.
func (tt *TCPTransporter) incomingLoop() {
	var err error

	tt.ln, err = net.ListenTCP("tcp", tt.tcpAddrs[tt.self])
	if err != nil {
		glog.Warning("Listen error: ", err)
		return
	}

	// Keep accepting connections.
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

		id, ok := tt.findIDByAddr(conn.RemoteAddr())
		if ok {
			tt.inConns[id] = conn
			go tt.readLoop(conn)
		}
	}
}

// Keep reading from one incoming connection,
// if the connection gets lost, then return.
func (tt *TCPTransporter) readLoop(conn *net.TCPConn) {
	for {
		select {
		case <-tt.stop:
			return
		default:
		}

		var b []byte
		_, err := conn.Read(b)
		if err != nil {
			// TODO: To detect connection lost error.
			remoteAddr := conn.RemoteAddr()
			glog.Warning("Read error from connection: ",
				remoteAddr, err)
			conn.Close()
			return
		}

		// TODO: Unmarshal and send to channel.
		fmt.Println(b)
	}
}

// Start the TCP transporter
func (tt *TCPTransporter) Start() error {
	// Start dial loop, wait for all outgoing connections.
	if err := tt.dialLoop(); err != nil {
		return err
	}
	// Start accept and read loop.
	go tt.incomingLoop()
	go tt.outgoingLoop()
	return nil
}

// Stop the TCP transporter
func (tt *TCPTransporter) Stop() {
	close(tt.stop)
	tt.ln.Close()
	for i := range tt.inConns {
		tt.inConns[i].Close()
	}
	for i := range tt.outConns {
		tt.outConns[i].Close()
	}
}
