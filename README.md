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
* github.com/golang/leveldb

Please "go get" them before running any code. Or you can run:

```bash
$ ./build.sh
```

### Run Test

```bash
$ ./test.sh
```
This will run several tests:

####Part I. static tests:

* Epaxos state machine
* Execution module
* Timeout module
* Persistent module

####Part II, dynamic tests:
* Log consistency test:
 * Single proposer, sending non-conflicted commands
 * Multiple proposer, sending non-conflicted commands
 * Multiple proposer, sending conflicted commands
 * Multiple proposer, sending conflicted commands, plus randomly timeouts


* Failover test:
 * Start a 3-node cluster
 * Randomly kill one node
 * Restart that node
 

### Run Demo

We have implemented a simple state machine to demonstrate consensus capability of our EPaxos
protocol. The state machine is a simple printer which prints the log in EPaxos instance spaces.

To run the demo

```bash
$ cd demo
$ go build
$ ./demo -id=[0|1|2]
```

In this demo, we will start 3 replicas, listening on different ports on localhost.
The replicas will propose commands concurrently and you shall see all of them printing
the same command sequences (logs).

[Note: there will be chance that one command is executed twice, this is because the "print" operation is not idempotent. For non-idempotent commands, they should be handled by the state machine. For simplicity, we didn't implement that in the demo. (Hopefully, this is very rare to happen in the test...)]

### Node Failure and Recovery

As stated in previous section, you can run 3 replicas and they will be continually print
same command as logs are replicated across them. Now if you kill one or two, and then restart
again, the failed ones will recovery from crash logs and reinstate to original. All the logs
will be made up later and begin proposing new commands too.


### What We Have Done

We have implemented the the basic EPaxos protocol. In out implementation, we divided the implementation into several modules:

* Core, the EPaxos state machine
* Execution Module
* Timeout Module
* Transportation Module
* Persistent Storage Module

Each module can be tested independently. Besides, in the core part, each state is a independent sub-module. We believe that such design can make the debugging much easier.

### What We Will Do

* Exploit the parallelism in EPaxos (Now the event dispatcher is single-threaded)
* Optimization on marshaling/unmarshaling
* Add TCP transporter
* Optimization on persistent module (Now the underlying storage is levelDB)
* Dynamic reconfiguration
* Longrun tests
* Benchmark

Contact
------
Send your questions or advices to *epaxos-dev@googlegroups.com*
