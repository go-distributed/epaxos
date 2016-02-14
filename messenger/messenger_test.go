package messenger

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-distributed/epaxos"
	"github.com/go-distributed/epaxos/message"
	"github.com/go-distributed/testify/assert"
)

func sendMessages(t *testing.T, mngr epaxos.Messenger, to uint8, msgs []message.Message) {
	for i := range msgs {
		assert.NoError(t, mngr.Send(to, msgs[i]))
	}
}

func randomReplicaID() uint8 {
	return uint8(rand.Int())
}

func randomInstanceID() uint64 {
	return uint64(rand.Int())
}

func randomCommands() message.Commands {
	// Add 1 here to avoid assert.Equal == false when len == 0.
	cmds := make([]message.Command, 1+rand.Intn(1024))
	for i := range cmds {
		randStr := fmt.Sprintf("%10d", rand.Int())
		cmds[i] = message.Command(randStr)
	}
	return cmds
}

func randomDependencies() message.Dependencies {
	// Add 1 here to avoid assert.Equal == false when len == 0.
	deps := make([]uint64, 1+rand.Intn(1024))
	for i := range deps {
		deps[i] = uint64(rand.Int())
	}
	return deps
}

func randomBallot() *message.Ballot {
	return message.NewBallot(
		uint32(rand.Int()),
		uint64(rand.Int()),
		uint8(rand.Int()))
}

func randomFrom() uint8 {
	return uint8(rand.Int())
}

func randomStatus() uint8 {
	return uint8(rand.Int())
}

func randomBool() bool {
	if rand.Int()%2 == 0 {
		return false
	}
	return true
}

func generateAllMessages(num int) []message.Message {
	ms := make([]message.Message, 0)

	for i := 0; i < num; i++ {
		pa := &message.PreAccept{
			ReplicaId:  randomReplicaID(),
			InstanceId: randomInstanceID(),
			Cmds:       randomCommands(),
			Deps:       randomDependencies(),
			Ballot:     randomBallot(),
			From:       randomFrom(),
		}
		ms = append(ms, pa)

		po := &message.PreAcceptOk{
			ReplicaId:  randomReplicaID(),
			InstanceId: randomInstanceID(),
			From:       randomFrom(),
		}
		ms = append(ms, po)

		pr := &message.PreAcceptReply{
			ReplicaId:  randomReplicaID(),
			InstanceId: randomInstanceID(),
			Deps:       randomDependencies(),
			Ballot:     randomBallot(),
			From:       randomFrom(),
		}
		ms = append(ms, pr)

		ac := &message.Accept{
			ReplicaId:  randomReplicaID(),
			InstanceId: randomInstanceID(),
			Cmds:       randomCommands(),
			Deps:       randomDependencies(),
			Ballot:     randomBallot(),
			From:       randomFrom(),
		}
		ms = append(ms, ac)

		ar := &message.AcceptReply{
			ReplicaId:  randomReplicaID(),
			InstanceId: randomInstanceID(),
			Ballot:     randomBallot(),
			From:       randomFrom(),
		}
		ms = append(ms, ar)

		pp := &message.Prepare{
			ReplicaId:  randomReplicaID(),
			InstanceId: randomInstanceID(),
			Ballot:     randomBallot(),
			From:       randomFrom(),
		}
		ms = append(ms, pp)

		ppr := &message.PrepareReply{
			ReplicaId:      randomReplicaID(),
			InstanceId:     randomInstanceID(),
			Status:         randomStatus(),
			Cmds:           randomCommands(),
			Deps:           randomDependencies(),
			Ballot:         randomBallot(),
			OriginalBallot: randomBallot(),
			IsFromLeader:   randomBool(),
			From:           randomFrom(),
		}
		ms = append(ms, ppr)

		cm := &message.Commit{
			ReplicaId:  randomReplicaID(),
			InstanceId: randomInstanceID(),
			Cmds:       randomCommands(),
			Deps:       randomDependencies(),
			From:       randomFrom(),
		}
		ms = append(ms, cm)
	}

	// Shuffle the messages.
	for i := range ms {
		index := rand.Intn(i + 1)
		ms[i], ms[index] = ms[index], ms[i]
	}

	return ms
}

func verifyMesseges(t *testing.T, sender, receiver int, receivedMsgs []message.Message, msgs []message.Message, done chan bool) {
	<-done // Wait for receiving completed.

	if sender == receiver {
		assert.Empty(t, receivedMsgs)
		return
	}
	assert.Equal(t, msgs, receivedMsgs)
}

func receiveMessages(t *testing.T, mngr epaxos.Messenger, msgs *[]message.Message, start, done chan bool) {
	go func() {
		for {
			<-start // Wait for trigger.
			<-time.After(time.Second * 10)
			done <- true // Tell that we completed receiving.
		}
	}()

	for {
		msg, err := mngr.Recv()
		assert.NoError(t, err)
		*msgs = append(*msgs, msg)
	}
}

func newGoGoProtobufHTTPMessengerGroup(t *testing.T, hostports map[uint8]string, chanSize int) []epaxos.Messenger {
	mngrs := make([]epaxos.Messenger, len(hostports))
	for i := range mngrs {
		var err error
		mngrs[i], err = NewGoGoProtobufHTTPMessenger(hostports, uint8(i), len(mngrs), chanSize)
		assert.NoError(t, err)
	}
	return mngrs
}

// Test Send of GoGoProtobufHTTPMessenger
func TestGoGoProtobufHTTPMessenger(t *testing.T) {
	numMessages := 1024

	hostports := make(map[uint8]string)
	hostports[0] = "localhost:8080"
	hostports[1] = "localhost:8081"
	hostports[2] = "localhost:8082"

	mngrs := newGoGoProtobufHTTPMessengerGroup(t, hostports, numMessages*10)

	for i := range mngrs {
		go mngrs[i].Start()
		time.Sleep(time.Second)
	}

	startChans := make([]chan bool, len(mngrs))
	for i := range startChans {
		startChans[i] = make(chan bool, 1)
	}

	done := make(chan bool, 1)
	receivedMsgs := make([]message.Message, 0)
	for i := range mngrs {
		go receiveMessages(t, mngrs[i], &receivedMsgs, startChans[i], done)
	}

	for i := range mngrs {
		for j := range mngrs {
			fmt.Printf("Generating %d messages\n", numMessages*8)
			receivedMsgs = receivedMsgs[:0] // Reset the slice.
			msgs := generateAllMessages(numMessages)

			t1 := time.Now().UnixNano()
			fmt.Printf("Sending %d messages from [%d] to [%d]\n", numMessages*8, i, j)
			sendMessages(t, mngrs[i], uint8(j), msgs)

			t2 := time.Now().UnixNano()

			fmt.Printf("Done! time elasped: %vms\n", (t2-t1)/1000000)
			startChans[j] <- true // Trigger receiving.

			fmt.Printf("Verifying %d messages\n", numMessages*8)
			verifyMesseges(t, i, j, receivedMsgs, msgs, done)
			fmt.Println("Messages look good!")
		}
	}

	for i := range mngrs {
		assert.NoError(t, mngrs[i].Stop())
	}
}
