package transporter

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"path"

	"github.com/go-distributed/epaxos/message"
	"github.com/golang/glog"
)

const (
	prefix             = "/epaxos"
	proposePath        = "/propose"
	preAcceptPath      = "/preAccept"
	preAcceptOkPath    = "/preAcceptOk"
	preAcceptReplyPath = "/preAcceptReply"
	acceptPath         = "/accept"
	acceptReplyPath    = "/acceptReply"
	commitPath         = "/commit"
	preparePath        = "/prepare"
	prepareReplyPath   = "/prepareReply"
)

type HTTPTransporter struct {
	addrStrings []string
	path        map[uint8]string
	self        uint8
	fastQuorum  uint8
	all         uint8
	replicaCh   chan message.Message
}

// Create a new http transporter.
func NewHTTPTransporter(addrStr []string, self uint8, size int) (*HTTPTransporter, error) {
	for i := range addrStr {
		// Validate the ip address.
		_, err := net.ResolveIPAddr("ip", addrStr[i])
		if err != nil {
			return nil, err
		}
	}
	t := &HTTPTransporter{
		addrStrings: addrStr,
		path:        make(map[uint8]string),
		self:        self,
		fastQuorum:  uint8(size) - 2,
		all:         uint8(size),
	}
	t.installPaths()
	t.installHandlers()
	return t, nil
}

// Install paths, so that we don't need to do join every time.
func (t *HTTPTransporter) installPaths() {
	t.path[message.ProposeMsg] = path.Join(prefix, proposePath)
	t.path[message.PreAcceptMsg] = path.Join(prefix, preAcceptPath)
	t.path[message.PreAcceptOkMsg] = path.Join(prefix, preAcceptOkPath)
	t.path[message.PreAcceptReplyMsg] = path.Join(prefix, preAcceptReplyPath)
	t.path[message.AcceptMsg] = path.Join(prefix, acceptPath)
	t.path[message.AcceptReplyMsg] = path.Join(prefix, acceptReplyPath)
	t.path[message.CommitMsg] = path.Join(prefix, commitPath)
	t.path[message.PrepareMsg] = path.Join(prefix, preparePath)
	t.path[message.PrepareReplyMsg] = path.Join(prefix, prepareReplyPath)
}

func (t *HTTPTransporter) installHandlers() {
	http.HandleFunc(t.path[message.ProposeMsg], t.proposeHandler)
	http.HandleFunc(t.path[message.PreAcceptMsg], t.preAcceptHandler)
	http.HandleFunc(t.path[message.PreAcceptOkMsg], t.preAcceptOkHandler)
	http.HandleFunc(t.path[message.PreAcceptReplyMsg], t.preAcceptReplyHandler)
	http.HandleFunc(t.path[message.AcceptMsg], t.acceptHandler)
	http.HandleFunc(t.path[message.AcceptReplyMsg], t.acceptReplyHandler)
	http.HandleFunc(t.path[message.CommitMsg], t.commitHandler)
	http.HandleFunc(t.path[message.PrepareMsg], t.prepareHandler)
	http.HandleFunc(t.path[message.PrepareReplyMsg], t.prepareReplyHandler)
}

// Send one message, it will block.
func (t *HTTPTransporter) Send(to uint8, msg message.Message) {
	if to > t.all { //TODO(yifan): Consider to use map instead of slice.
		panic("")
	}
	// TODO(yifan): Support https.
	targetURL := fmt.Sprintf("http://%s%s", t.addrStrings[to], t.path[msg.Type()])

	// Marshal the message.
	data, err := msg.MarshalBinary()
	if err != nil {
		glog.Warning("HTTPTransporter: Marshal error for: ",
			message.MessageTypeString(msg),
			err)
		return
	}
	_, err = http.Post(targetURL, "application/protobuf", bytes.NewBuffer(data))
	if err != nil {
		glog.Warning("HTTPTransporter: Post error for: ",
			message.MessageTypeString(msg),
			err)
		return
	}
}

// Send one message to multiple receivers, it will block.
func (t *HTTPTransporter) MulticastFastquorum(msg message.Message) {
	skip := uint8(rand.Intn(int(t.all)))
	if skip == t.self {
		skip = (skip + 1) % t.all
	}

	for i := uint8(0); i < t.all; i++ {
		if i == t.self || i == skip {
			continue
		}
		go t.Send(i, msg)
	}
}

// Send one message to all receivers, it will block.
func (t *HTTPTransporter) Broadcast(msg message.Message) {
	for i := uint8(0); i < t.all; i++ {
		if i == t.self {
			continue
		}
		go t.Send(i, msg)
	}
}

// Register a channel to communicate with replica.
func (t *HTTPTransporter) RegisterChannel(ch chan message.Message) {
	t.replicaCh = ch
}

// Start the transporter, it will block until success or failure.
func (t *HTTPTransporter) Start() error {
	return http.ListenAndServe(t.addrStrings[t.self], nil)
}

// Stop the transporter.
func (t *HTTPTransporter) Stop() {}

// Handlers
func (t *HTTPTransporter) handleMessage(msg message.Message, r *http.Request) {
	var data []byte
	n, err := r.Body.Read(data)
	if err != nil {
		glog.Warning("HTTPTransporter: Read HTTP body error for: ",
			message.MessageTypeString(msg),
			err)
		return
	}
	if err = msg.UnmarshalBinary(data[:n]); err != nil {
		glog.Warning("HTTPTransporter: Unmarshal error for: ",
			message.MessageTypeString(msg),
			err)
		return
	}
	t.replicaCh <- msg
}

func (t *HTTPTransporter) proposeHandler(w http.ResponseWriter, r *http.Request) {
	panic("Not implemented yet")
}

func (t *HTTPTransporter) preAcceptHandler(w http.ResponseWriter, r *http.Request) {
	msg := new(message.PreAccept)
	t.handleMessage(msg, r)
}

func (t *HTTPTransporter) preAcceptOkHandler(w http.ResponseWriter, r *http.Request) {
	msg := new(message.PreAcceptOk)
	t.handleMessage(msg, r)
}

func (t *HTTPTransporter) preAcceptReplyHandler(w http.ResponseWriter, r *http.Request) {
	msg := new(message.PreAcceptReply)
	t.handleMessage(msg, r)
}

func (t *HTTPTransporter) acceptHandler(w http.ResponseWriter, r *http.Request) {
	msg := new(message.Accept)
	t.handleMessage(msg, r)
}

func (t *HTTPTransporter) acceptReplyHandler(w http.ResponseWriter, r *http.Request) {
	msg := new(message.AcceptReply)
	t.handleMessage(msg, r)
}

func (t *HTTPTransporter) commitHandler(w http.ResponseWriter, r *http.Request) {
	msg := new(message.Commit)
	t.handleMessage(msg, r)
}

func (t *HTTPTransporter) prepareHandler(w http.ResponseWriter, r *http.Request) {
	msg := new(message.Prepare)
	t.handleMessage(msg, r)
}

func (t *HTTPTransporter) prepareReplyHandler(w http.ResponseWriter, r *http.Request) {
	msg := new(message.PrepareReply)
	t.handleMessage(msg, r)
}
