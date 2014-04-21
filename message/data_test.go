package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	p := &PreAccept{}
	pr := &PreAcceptReply{}
	po := &PreAcceptOk{}
	a := &Accept{}
	ar := &AcceptReply{}
	c := &Commit{}
	pp := &Prepare{}
	ppr := &PrepareReply{}
	pps := &Propose{}

	assert.Equal(t, p.Type(), PreAcceptMsg)
	assert.Equal(t, pr.Type(), PreAcceptReplyMsg)
	assert.Equal(t, po.Type(), PreAcceptOkMsg)
	assert.Equal(t, a.Type(), AcceptMsg)
	assert.Equal(t, ar.Type(), AcceptReplyMsg)
	assert.Equal(t, c.Type(), CommitMsg)
	assert.Equal(t, pp.Type(), PrepareMsg)
	assert.Equal(t, ppr.Type(), PrepareReplyMsg)
	assert.Equal(t, pps.Type(), ProposeMsg)
}

func TestContent(t *testing.T) {
	p := &PreAccept{}
	pr := &PreAcceptReply{}
	po := &PreAcceptOk{}
	a := &Accept{}
	ar := &AcceptReply{}
	c := &Commit{}
	pp := &Prepare{}
	ppr := &PrepareReply{}
	pps := &Propose{}

	assert.Equal(t, p.Content(), p)
	assert.Equal(t, pr.Content(), pr)
	assert.Equal(t, po.Content(), po)
	assert.Equal(t, a.Content(), a)
	assert.Equal(t, ar.Content(), ar)
	assert.Equal(t, c.Content(), c)
	assert.Equal(t, pp.Content(), pp)
	assert.Equal(t, ppr.Content(), ppr)
	assert.Equal(t, pps.Content(), pps)
}

func TestReplica(t *testing.T) {
	p := &PreAccept{ReplicaId: 1}
	pr := &PreAcceptReply{ReplicaId: 2}
	po := &PreAcceptOk{ReplicaId: 3}
	a := &Accept{ReplicaId: 4}
	ar := &AcceptReply{ReplicaId: 5}
	c := &Commit{ReplicaId: 6}
	pp := &Prepare{ReplicaId: 7}
	ppr := &PrepareReply{ReplicaId: 8}
	pps := &Propose{ReplicaId: 9}

	assert.Equal(t, p.Replica(), uint8(1))
	assert.Equal(t, pr.Replica(), uint8(2))
	assert.Equal(t, po.Replica(), uint8(3))
	assert.Equal(t, a.Replica(), uint8(4))
	assert.Equal(t, ar.Replica(), uint8(5))
	assert.Equal(t, c.Replica(), uint8(6))
	assert.Equal(t, pp.Replica(), uint8(7))
	assert.Equal(t, ppr.Replica(), uint8(8))
	assert.Equal(t, pps.Replica(), uint8(9))
}

func TestInstance(t *testing.T) {
	p := &PreAccept{InstanceId: 1}
	pr := &PreAcceptReply{InstanceId: 2}
	po := &PreAcceptOk{InstanceId: 3}
	a := &Accept{InstanceId: 4}
	ar := &AcceptReply{InstanceId: 5}
	c := &Commit{InstanceId: 6}
	pp := &Prepare{InstanceId: 7}
	ppr := &PrepareReply{InstanceId: 8}
	pps := &Propose{InstanceId: 9}

	assert.Equal(t, p.Instance(), uint64(1))
	assert.Equal(t, pr.Instance(), uint64(2))
	assert.Equal(t, po.Instance(), uint64(3))
	assert.Equal(t, a.Instance(), uint64(4))
	assert.Equal(t, ar.Instance(), uint64(5))
	assert.Equal(t, c.Instance(), uint64(6))
	assert.Equal(t, pp.Instance(), uint64(7))
	assert.Equal(t, ppr.Instance(), uint64(8))
	assert.Equal(t, pps.Instance(), uint64(9))
}
