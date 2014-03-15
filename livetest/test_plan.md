## Test Plan
### Basic Test (3 replicas)

* Non-conflict commands, 1 receiver **(Pass)**
 * Expect: All replicas have same correct logs(cmds, seq, deps) eventually

* Non-conflict commands, 3 receivers

 * Expect: All replicas have same correct logs(cmds, seq, deps) eventually

* Conflict commands, 1 receiver **(Half Pass)**

 * Expect: All replicas have same correct logs(cmds, seq, deps) eventually

* Conflict commands, 3 receivers

 * Expect: All replicas have same correct logs(cmds, seq, deps) eventually

### Basic Robust Test (3 replicas)

* Non-conflict commands, 1 receiver, randomly kill 1 replica (not the receiver)

 * Expect: The receiver should continue working correctly

* Non-conflict commands, 3 receivers, randomly kill 1 replica (not the receiver)

 * Expect: The receiver should continue working correctly

* Conflict commands, 1 receiver, randomly kill 1 replica (not the receiver)

 * Expect: The receiver should continue working correctly

* Conflict commands, 3 receivers, randomly kill 1 replica (not the receiver)

 * Expect: The receiver should continue working correctly

### To Be Continued...
 