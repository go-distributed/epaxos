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

func TestMarshalUnmarshalProtobuf(t *testing.T) {
	// Tests for PreAccept.
	p0 := &PreAccept{
		ReplicaId:  1,
		InstanceId: 2,
		Cmds: Commands{
			Command("Hello"),
			Command("World"),
		},
		Deps:   Dependencies{1, 2, 3},
		Ballot: NewBallot(1, 2, 3),
		From:   1,
	}

	data, err := p0.MarshalProtobuf()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	p1 := new(PreAccept)
	assert.NoError(t, p1.UnmarshalProtobuf(data))
	assert.Equal(t, p0, p1)

	// Tests for PreAcceptOk
	p2 := &PreAcceptOk{
		ReplicaId:  1,
		InstanceId: 2,
		From:       1,
	}

	data, err = p2.MarshalProtobuf()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	p3 := new(PreAcceptOk)
	assert.NoError(t, p3.UnmarshalProtobuf(data))
	assert.Equal(t, p2, p3)

	// Tests for PreAcceptReply
	p4 := &PreAcceptReply{
		ReplicaId:  1,
		InstanceId: 2,
		Deps:       Dependencies{1, 2, 3},
		Ballot:     NewBallot(1, 2, 3),
		From:       1,
	}

	data, err = p4.MarshalProtobuf()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	p5 := new(PreAcceptReply)
	assert.NoError(t, p5.UnmarshalProtobuf(data))
	assert.Equal(t, p4, p5)

	// Tests for Accept.
	a0 := &Accept{
		ReplicaId:  1,
		InstanceId: 2,
		Cmds: Commands{
			Command("Hello"),
			Command("World"),
		},
		Deps:   Dependencies{1, 2, 3},
		Ballot: NewBallot(1, 2, 3),
		From:   1,
	}

	data, err = a0.MarshalProtobuf()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	a1 := new(Accept)
	assert.NoError(t, a1.UnmarshalProtobuf(data))
	assert.Equal(t, a0, a1)

	// Tests for AcceptReply.
	a2 := &AcceptReply{
		ReplicaId:  1,
		InstanceId: 2,
		Ballot:     NewBallot(1, 2, 3),
		From:       1,
	}

	data, err = a2.MarshalProtobuf()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	a3 := new(AcceptReply)
	assert.NoError(t, a3.UnmarshalProtobuf(data))
	assert.Equal(t, a2, a3)

	// Tests for Commit.
	c0 := &Commit{
		ReplicaId:  1,
		InstanceId: 2,
		Cmds: Commands{
			Command("Hello"),
			Command("World"),
		},
		Deps: Dependencies{1, 2, 3},
		From: 1,
	}

	data, err = c0.MarshalProtobuf()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	c1 := new(Commit)
	assert.NoError(t, c1.UnmarshalProtobuf(data))
	assert.Equal(t, c0, c1)

	// Tests for Prepare.
	p6 := &Prepare{
		ReplicaId:  1,
		InstanceId: 2,
		Ballot:     NewBallot(1, 2, 3),
		From:       1,
	}

	data, err = p6.MarshalProtobuf()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	p7 := new(Prepare)
	assert.NoError(t, p7.UnmarshalProtobuf(data))
	assert.Equal(t, p6, p7)

	// Tests for PrepareReply.
	p8 := &PrepareReply{
		ReplicaId:  1,
		InstanceId: 2,
		Status:     1, // TODO(yifan): Change to exported state.
		Cmds: Commands{
			Command("Hello"),
			Command("World"),
		},
		Deps:           Dependencies{1, 2, 3},
		Ballot:         NewBallot(1, 2, 3),
		OriginalBallot: NewBallot(3, 2, 1),
		IsFromLeader:   true,
		From:           1,
	}

	data, err = p8.MarshalProtobuf()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	p9 := new(PrepareReply)
	assert.NoError(t, p9.UnmarshalProtobuf(data))
	assert.Equal(t, p8, p9)

	// Tests for Propose
	p10 := new(Propose)

	data, err = p10.MarshalProtobuf()
	assert.Nil(t, data)
	assert.Error(t, err)

	p11 := new(Propose)
	assert.Error(t, p11.UnmarshalProtobuf(data))

	// Test for Timeout
	t0 := new(Timeout)

	data, err = t0.MarshalProtobuf()
	assert.Nil(t, data)
	assert.Error(t, err)

	t1 := new(Propose)
	assert.Error(t, t1.UnmarshalProtobuf(data))
}
