Instance State Machine
======

This document describes the state machine for each instance status's processing.


Instance Status
------
Instace status include:
- Nil
- preparing
- preAccepted
- accepted
- committed
- executed



How Instance State Transition
------
*Nil* to
- *preparing*: 	dependency required
- *preAccepted*: receive proposal, receive preAccept
- *accepted*: receive accept
- *committed*: receive commit

*preAccepted* to
- *accepted*: slow path, receive accept
- *committed*: fast path, receive commit
- *preparing*: timeout in sending preAccept, dependency required

*accepted* to
- *committed*: majority agree, receive commmit
- *preparing*: timeout in sending accept, dependency required

*preparing* to
- *preAccepted*: (<= N/2 preAccepted), all noop, receive preAccept
- *accepted*: (> N/2 preAccepted), (accepted in prepare reply), receive accept
- *committed*: (committed in prepare reply), receive commit
- *preparing*: timeout in sending prepare

*committed* to
- *executed*: 



Transition Conditions Explained
------
Transition conditions:
- *dependency required*, happens when other committed instance depends on this instance and require it to be committed.
- *fast path*, happens when **initial** leader receives identical preAccept replies from fast quorum.
- *slow path*, happens when leader receives majority votes but it doesn't satisfy fast path conditions.
- *timeout*, happens when timeout in waiting for replies.
- *< N/2 preAccept*, happens when in preparing, less than N/2 replica reply preAccept messages.
- *>= N/2 preAccept*, happens when in preparing, majority reply preAccept messages.
- *all noop*, happens when all prepare replies of no-op commands.
- *majority agree*, happens when >= N/2 replica agree on the message you sent.
- *receive < XXX >*, happens when receiving message of < XXX > type and ballot of message is larger than local one (except commit).



Nil Status Explained
------
An instance's status is NilStatus when:
- it doesn't exist, or
- it doesn't know any information about this instance except its ballot.
