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

// This tests the functionality of NewTCPTransporter().
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
	assert.NoError(t, err)
	assert.NotNil(t, tt)
	tt.Stop()
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
	tt.Stop()
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
	tt.Stop()
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
	tt.Stop()
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
	go tt.outgoingLoop()
	tt.Send(1, nil) // Send fail and dial should fail
	time.Sleep(time.Microsecond * 500)
	assert.Equal(t, origConn, tt.outConns[1])

	done2 = runRecvServer(":8081")
	time.Sleep(time.Millisecond * 500)
	tt.Send(1, nil) // This time should fail at first, and then dial successfully.
	time.Sleep(time.Millisecond * 500)

	assert.NotEqual(t, origConn, tt.outConns[1])
	<-done2
	tt.Stop()
}

// This tests the functionality of findIDByAddr
func TestFindIDByAddr(t *testing.T) {
	tt, err := NewTCPTransporter(
		[]string{"localhost:8080", "localhost:8081", "localhost:8082"},
		0,
		3)
	assert.NoError(t, err)

	addr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
	assert.NoError(t, err)
	id, ok := tt.findIDByAddr(addr)
	assert.True(t, ok)
	assert.Equal(t, id, uint8(0))

	addr, err = net.ResolveTCPAddr("tcp", "localhost:8081")
	assert.NoError(t, err)
	id, ok = tt.findIDByAddr(addr)
	assert.True(t, ok)
	assert.Equal(t, id, uint8(1))

	addr, err = net.ResolveTCPAddr("tcp", "localhost:8082")
	assert.NoError(t, err)
	id, ok = tt.findIDByAddr(addr)
	assert.True(t, ok)
	assert.Equal(t, id, uint8(2))

	addr, err = net.ResolveTCPAddr("tcp", "localhost:8083")
	assert.NoError(t, err)
	id, ok = tt.findIDByAddr(addr)
	assert.False(t, ok)
	assert.Equal(t, id, uint8(0))

	tt.Stop()
}

//// This tests the functionality of run().
//func TestRun(t *testing.T) {
//	tt, err := NewTCPTransporter(
//		[]string{"localhost:8080", "localhost:8081", "localhost:8082"},
//		0,
//		3)
//	assert.NoError(t, err)
//
//	go tt.run()
//
//	time.Sleep(time.Millisecond * 500)
//
//	localAddr, err := net.ResolveTCPAddr("tcp", "localhost:8081")
//	assert.NoError(t, err)
//
//	remoteAddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
//	assert.NoError(t, err)
//
//	conn, err := net.DialTCP("tcp", localAddr, remoteAddr)
//
//	time.Sleep(time.Millisecond * 500)
//
//	assert.NoError(t, err)
//	assert.NotNil(t, conn)
//
//	assert.NotNil(t, tt.inConns[1])
//	conn.Close()
//	tt.Stop()
//}
