## Test Plan
### Basic Test (3 replicas)

* Non-conflict commands, 1 proposer **(PASS)**
 * Expect: All replicas have same correct logs(cmds, deps) eventually

* Non-conflict commands, 3 proposers **(PASS)**

 * Expect: All replicas have same correct logs(cmds, deps) eventually

* Conflict commands, 1 proposer **(TODO)**

 * Expect: All replicas have same correct logs(cmds, deps) eventually

* Conflict commands, 3 proposers

 * Expect: All replicas have same correct logs(cmds, deps) eventually

### Basic Robust Test (3 replicas)

* Non-conflict commands, 1 proposer, randomly kill 1 replica (not the proposer)

 * Expect: The proposer should continue working correctly

* Non-conflict commands, 3 proposers, randomly kill 1 replica (not the proposer)

 * Expect: The proposer should continue working correctly

* Conflict commands, 1 proposer, randomly kill 1 replica (not the proposer)

 * Expect: The proposer should continue working correctly

* Conflict commands, 3 proposers, randomly kill 1 replica (not the proposer)

 * Expect: The proposer should continue working correctly

### To Be Continued...
 
