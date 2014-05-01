epaxos
======
[![Build Status](https://drone.io/github.com/go-distributed/epaxos/status.png)](https://drone.io/github.com/go-distributed/epaxos/latest)

[![Coverage Status](https://coveralls.io/repos/go-distributed/epaxos/badge.png?branch=master)](https://coveralls.io/r/go-distributed/epaxos?branch=master)

A Go implementation of epaxos

Summary
------
This section is credited to [efficient/epaxos](https://github.com/efficient/epaxos).

### What is EPaxos?


EPaxos is an efficient, leaderless replication protocol. The name stands for *Egalitarian Paxos* -- EPaxos is based
on the Paxos consensus algorithm. As such, it can tolerate up to F concurrent replica failures with 2F+1 total replicas.

### How does EPaxos differ from Paxos and other Paxos variants?

To function effectively as a replication protocol, Paxos has to rely on a stable leader replica (this optimization is known as Multi-Paxos). The leader can become a bottleneck for performance: it has to handle more messages than the other replicas, and remote clients have to contact the leader, thus experiencing higher latency. Other Paxos variants either also rely on a stable leader, or have a pre-established scheme that allows different replicas to take turns in proposing commands (such as Mencius). This latter scheme
suffers from tight coupling of the performance of the system from that of every replica -- i.e., the system runs at the speed of the slowest replica.

EPaxos is an efficient, leaderless protocol. It provides **strong consistency with optimal wide-area latency, perfect load-balancing across replicas (both in the local and the wide area), and constant availability for up to F failures**. EPaxos also decouples the performance of the slowest replicas from that of the fastest, so it can better tolerate slow replicas than previous protocols.

### How does EPaxos work?

We have [an SOSP 2013 paper](http://dl.acm.org/ft_gateway.cfm?id=2517350&ftid=1403953&dwn=1) that describes EPaxos in detail.

A simpler, more straightforward explanation is coming here soon.


How To Run And Test
------

### Dependencies

This repository has following dependencies:

* github.com/stretchr/testify
* github.com/golang/glog
* code.google.com/p/leveldb-go/leveldb

Please "go get" them before running any code.


### Run Test

In *replica/*, *message/*, *persistent/* directories, run

    go test

These tests are unit-test code for our instance and replica module

In *livetest/*, run

    go test

These tests are used to verify our replication and consistency correctness.


### Run Demo

We have written a very simple code to demonstrate consensus capability of our EPaxos
protocol.

Go to *demo/*, then run

    go run server.go [0|1|2]

The final one is the id of replica.

We have predefined 3 replicas listening on different ports on localhost.
Each of them will propose random commands intervally and you shall see all of them printing
the same command sequences (logs).

### Node Failure and Recovery

As stated in previous section, you can run 3 replicas and they will be continually print
same command as logs are replicated across them. Now if you kill one or two, and then restart
again, the failed ones will recovery from crash logs and reinstate to original. All the logs
will be made up later and begin proposing new commands too.


Contact
------
Send your questions or advices to *epaxos-dev@googlegroups.com*
