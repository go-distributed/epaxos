package transporter

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func runRecvServer(hostport string) <-chan struct{} {
	ch := make(chan struct{})

	go func() {
		ln, err := net.Listen("tcp", hostport)
		if err != nil {
			panic("")
		}

		conn, err := ln.Accept()
		if err != nil || conn == nil {
			panic("")
		}

		conn.Close()
		ln.Close()
		close(ch)
	}()

	return ch
}

// This tests the functionality of NewTCPTransporter()
func TestNewTCPTransporter(t *testing.T) {
	tt, err := NewTCPTransporter(
		[]string{"localhost:d", "localhost:8081", "localhost:8082"},
		0,
		3)
	assert.Nil(t, tt)
	assert.Error(t, err)

	tt, err = NewTCPTransporter(
		[]string{"localhost:8080", "localhost:8081", "localhost:8082"},
		0,
		3)
	assert.Nil(t, err)
	assert.NotNil(t, tt)
}

// This tests the dial() function
func TestDial(t *testing.T) {
	tt, err := NewTCPTransporter(
		[]string{"localhost:8080", "localhost:8081", "localhost:8082"},
		0,
		3)
	assert.NoError(t, err)

	err = tt.dial(1)
	assert.Error(t, err)

	go func() {
		time.Sleep(1 * time.Second)
		err := tt.dial(1)
		assert.NoError(t, err)
		assert.NotNil(t, tt.outConns[1])
	}()

	done := runRecvServer(":8081")
	<-done
}

// This tests the failure handling functionality of the dialLoop()
func TestDialLoopFail(t *testing.T) {
	tt, err := NewTCPTransporter(
		[]string{"localhost:8080", "localhost:8081", "localhost:8082"},
		0,
		3)
	assert.NoError(t, err)

	// 1, no connections
	assert.Error(t, tt.dialLoop())

	// 2, only one connection
	done := runRecvServer(":8081")
	assert.Error(t, tt.dialLoop())
	<-done
	assert.NotNil(t, tt.outConns[1])

}

// This tests the successful behaviour of the dialLoop
func TestDialLoopSucceed(t *testing.T) {
	tt, err := NewTCPTransporter(
		[]string{"localhost:8080", "localhost:8081", "localhost:8082"},
		0,
		3)
	assert.NoError(t, err)

	done1 := runRecvServer(":8081")
	done2 := runRecvServer(":8082")

	assert.NoError(t, tt.dialLoop())
	<-done1
	<-done2
	assert.NotNil(t, tt.outConns[1])
	assert.NotNil(t, tt.outConns[2])
}

// This tests the Send() function, if a send fails, it should try to
// reconnect by calling the dial()
// Note: Now it is the non-marshaler version
func TestSend(t *testing.T) {
	tt, err := NewTCPTransporter(
		[]string{"localhost:8080", "localhost:8081", "localhost:8082"},
		0,
		3)
	assert.NoError(t, err)

	done1 := runRecvServer(":8081")
	done2 := runRecvServer(":8082")

	assert.NoError(t, tt.dialLoop())

	<-done1
	<-done2

	origConn := tt.outConns[1]

	tt.outConns[1].SetWriteDeadline(time.Now())
	tt.Send(1, nil) // send fail and dial should fail
	time.Sleep(time.Microsecond * 500)
	assert.Equal(t, origConn, tt.outConns[1])

	done2 = runRecvServer(":8081")
	time.Sleep(time.Millisecond * 500)
	tt.Send(1, nil) // this time should fail at first, and then dial successfully
	time.Sleep(time.Millisecond * 500)

	assert.NotEqual(t, origConn, tt.outConns[1])
}

// TODO: test run()
func TestRun(t *testing.T) {
	
}
