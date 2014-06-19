package transporter

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"

	"github.com/golang/glog"
)

const prefix = "/epaxos"

const defaultChanSize = 1024

var (
	internalMessagePath string
	proposeRequestPath  string
)

// The buffer size for the internal channel.

type rawMessage struct {
	data []byte
	err  error
}

type HTTPTransporter struct {
	self           string
	path           map[uint8]string
	rawMessageChan chan *rawMessage

	mux    *http.ServeMux
	client *http.Client
}

// Create a new http transporter.
func NewHTTPTransporter(self string, chanSize int) (*HTTPTransporter, error) {
	if chanSize <= 0 {
		chanSize = defaultChanSize
	}

	t := &HTTPTransporter{
		self:           self,
		path:           make(map[uint8]string),
		rawMessageChan: make(chan *rawMessage, chanSize),

		mux:    http.NewServeMux(),
		client: new(http.Client),
	}
	t.installPaths()
	t.installHandlers()
	return t, nil
}

// Install paths, so that we don't need to do join every time.
func (t *HTTPTransporter) installPaths() {
	internalMessagePath = path.Join(prefix, "/internalMessage")
	proposeRequestPath = path.Join(prefix, "/proposeRequest")
}

func (t *HTTPTransporter) installHandlers() {
	t.mux.HandleFunc(internalMessagePath, t.internalMessageHandler)
	t.mux.HandleFunc(proposeRequestPath, t.proposeHandler)
}

// Send an encoded message to the host:port.
// This will block. We don't need the msgType here
// because we assume the message is self-explained.
func (t *HTTPTransporter) Send(hostport string, b []byte) error {
	if hostport == t.self {
		glog.Warning("Sending message to self!")
		return nil
	}

	targetURL := fmt.Sprintf("http://%s%s", hostport, internalMessagePath)

	// Send POST.
	resp, err := t.client.Post(targetURL, "application/epaxos", bytes.NewReader(b))
	if resp == nil || err != nil {
		glog.Warning("HTTPTransporter: Failed to POST ", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}

// Receive an encoded message from some peer.
// Return the bytes form of the message.
func (t *HTTPTransporter) Recv() (b []byte, err error) {
	rawMsg := <-t.rawMessageChan
	return rawMsg.data, rawMsg.err
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
	return nil
}

// Destroy the transporter.
func (t *HTTPTransporter) Destroy() error {
	close(t.rawMessageChan)
	return nil
}

// Handle incoming messages (except for propose).
func (t *HTTPTransporter) internalMessageHandler(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		glog.Warning("HTTPTransporter: Read HTTP body error for: ", err)
	}
	t.rawMessageChan <- &rawMessage{b, err}
}

// Handle incoming propose requests.
func (t *HTTPTransporter) proposeHandler(w http.ResponseWriter, r *http.Request) {
	panic("Not implemented yet")
}
