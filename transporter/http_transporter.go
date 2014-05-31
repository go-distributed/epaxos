package transporter

import (
	"bytes"
	"fmt"
	"io/ioutil"
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

// The buffer size for the internal channel.
const defaultChanSize = 1024

type rawMessage struct {
	mtype uint8
	data  []byte
	err   error
}

type HTTPTransporter struct {
	self           string
	path           map[uint8]string
	rawMessageChan chan *rawMessage

	mux    *http.ServeMux
	client *http.Client
}

// Create a new http transporter.
func NewHTTPTransporter(self string) (*HTTPTransporter, error) {
	t := &HTTPTransporter{
		self:           self,
		path:           make(map[uint8]string),
		rawMessageChan: make(chan *rawMessage, defaultChanSize),

		mux:    http.NewServeMux(),
		client: new(http.Client),
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
	t.mux.HandleFunc(t.path[message.ProposeMsg], t.proposeHandler)
	t.mux.HandleFunc(t.path[message.PreAcceptMsg], t.preAcceptHandler)
	t.mux.HandleFunc(t.path[message.PreAcceptOkMsg], t.preAcceptOkHandler)
	t.mux.HandleFunc(t.path[message.PreAcceptReplyMsg], t.preAcceptReplyHandler)
	t.mux.HandleFunc(t.path[message.AcceptMsg], t.acceptHandler)
	t.mux.HandleFunc(t.path[message.AcceptReplyMsg], t.acceptReplyHandler)
	t.mux.HandleFunc(t.path[message.CommitMsg], t.commitHandler)
	t.mux.HandleFunc(t.path[message.PrepareMsg], t.prepareHandler)
	t.mux.HandleFunc(t.path[message.PrepareReplyMsg], t.prepareReplyHandler)
}

// Send an encoded message to the host:port.
// This will block. We need the msgType here because
// we assume the message is not self-explained.
func (t *HTTPTransporter) Send(hostport string, msgType uint8, b []byte) error {
	if hostport == t.self {
		glog.Warning("Sending message to self!")
		return nil
	}

	// TODO(yifan): Support https.
	targetURL := fmt.Sprintf("http://%s%s", hostport, t.path[msgType])

	// Send POST.
	resp, err := t.client.Post(targetURL, "application/protobuf", bytes.NewBuffer(b))
	if resp == nil || err != nil {
		glog.Warning("HTTPTransporter: Post error for: ",
			message.TypeToString(msgType),
			err)
		return err
	}
	defer resp.Body.Close()
	return nil
}

// Receive an encoded message from some peer.
// Return the type of the message and the content.
// We need the msgType here because we assume the
// message is not self-explained.
func (t *HTTPTransporter) Recv() (msgType uint8, b []byte, err error) {
	rawMsg := <-t.rawMessageChan
	return rawMsg.mtype, rawMsg.data, rawMsg.err
}

// Start the transporter, it will block until success or failure.
func (t *HTTPTransporter) Start() error {
	err := http.ListenAndServe(t.self, t.mux)
	if err != nil {
		return err
	}
	return nil
}

// Stop the transporter.
func (t *HTTPTransporter) Stop() error {
	close(t.rawMessageChan)
	return nil
}

// Message handlers
func (t *HTTPTransporter) proposeHandler(w http.ResponseWriter, r *http.Request) {
	panic("Not implemented yet")
}

func (t *HTTPTransporter) preAcceptHandler(w http.ResponseWriter, r *http.Request) {
	t.handleMessage(message.PreAcceptMsg, r)
}

func (t *HTTPTransporter) preAcceptOkHandler(w http.ResponseWriter, r *http.Request) {
	t.handleMessage(message.PreAcceptOkMsg, r)
}

func (t *HTTPTransporter) preAcceptReplyHandler(w http.ResponseWriter, r *http.Request) {
	t.handleMessage(message.PreAcceptReplyMsg, r)
}

func (t *HTTPTransporter) acceptHandler(w http.ResponseWriter, r *http.Request) {
	t.handleMessage(message.AcceptMsg, r)
}

func (t *HTTPTransporter) acceptReplyHandler(w http.ResponseWriter, r *http.Request) {
	t.handleMessage(message.AcceptReplyMsg, r)
}

func (t *HTTPTransporter) commitHandler(w http.ResponseWriter, r *http.Request) {
	t.handleMessage(message.CommitMsg, r)
}

func (t *HTTPTransporter) prepareHandler(w http.ResponseWriter, r *http.Request) {
	t.handleMessage(message.PrepareMsg, r)
}

func (t *HTTPTransporter) prepareReplyHandler(w http.ResponseWriter, r *http.Request) {
	t.handleMessage(message.PrepareReplyMsg, r)
}

// Send the messag type and raw bytes into the internal channel.
func (t *HTTPTransporter) handleMessage(msgType uint8, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		glog.Warning("HTTPTransporter: Read HTTP body error for: ",
			message.TypeToString(msgType),
			err)
	}

	t.rawMessageChan <- &rawMessage{
		mtype: msgType,
		data:  b,
		err:   err,
	}
}
