package transporter

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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

	ln, err := net.Listen("tcp", ":8081")
	assert.NoError(t, err)

	conn, err := ln.Accept()
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	ln.Close()
}

func TestDialLoopFail(t *testing.T) {
	tt, err := NewTCPTransporter(
		[]string{"localhost:8080", "localhost:8081", "localhost:8082"},
		0,
		3)
	assert.NoError(t, err)

	// 1, no connections
	dialDone := make(chan struct{})
	errChan := make(chan error, 1)
	tt.dialLoop(dialDone, errChan)
	<-dialDone
	assert.Error(t, <-errChan)

	// 2, only one connection
	ok := make(chan struct{})
	go func(ok chan struct{}) {
		ln, err := net.Listen("tcp", ":8081")
		assert.NoError(t, err)

		conn, err := ln.Accept()
		assert.NoError(t, err)
		assert.NotNil(t, conn)
		ln.Close()
		close(ok)
	}(ok)

	dialDone = make(chan struct{})
	errChan = make(chan error, 1)
	tt.dialLoop(dialDone, errChan)
	<-dialDone
	assert.Error(t, <-errChan)
	<-ok

	assert.NotNil(t, tt.outConns[1])

}

func TestDialLoopSucceed(t *testing.T) {
	tt, err := NewTCPTransporter(
		[]string{"localhost:8080", "localhost:8081", "localhost:8082"},
		0,
		3)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		ln, err := net.Listen("tcp", ":8081")
		assert.NoError(t, err)

		conn, err := ln.Accept()
		assert.NoError(t, err)
		assert.NotNil(t, conn)

		ln.Close()
		wg.Done()
	}()

	go func() {
		ln, err := net.Listen("tcp", ":8082")
		assert.NoError(t, err)

		conn, err := ln.Accept()
		assert.NoError(t, err)
		assert.NotNil(t, conn)

		ln.Close()
		wg.Done()
	}()

	dialDone := make(chan struct{})
	errChan := make(chan error, 1)
	tt.dialLoop(dialDone, errChan)
	<-dialDone
	assert.Equal(t, len(errChan), 0)
	wg.Wait()

	assert.NotNil(t, tt.outConns[1])
	assert.NotNil(t, tt.outConns[2])
}

// TODO:: test run
