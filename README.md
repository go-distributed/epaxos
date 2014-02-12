epaxos
======
[![Build Status](https://drone.io/github.com/go-epaxos/epaxos/status.png)](https://drone.io/github.com/go-epaxos/epaxos/latest)

[![Coverage Status](https://coveralls.io/repos/go-epaxos/epaxos/badge.png)](https://coveralls.io/r/go-epaxos/epaxos)

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


Repository File Structure
------

We are documenting directories description here.

* **replica/**

  The epaxos replica module.

* **data/**

  Message, commands and other related data module.

* **test/**

  Testing infrastructure.

* **doc/**

  General concepts explanation and blue print documentation.
  For detailed documentation, please refer to individual moduel and files.

Contact
------
Send your questions or advices to *epaxos-dev@googlegroups.com*
