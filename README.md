# Quorum Broadcast #

Replaces Zookeeper QuorumCnxManager and improves FastLeaderElection(FLE) 
using a VoteView.
Implemented new leader election to show case how old FLE bugs can be fixed 
and attempts in a crude fashion to implement leader election for 
[omega/f-accessibility](https://www.microsoft.com/en-us/research/publication/omega-meets-paxos-leader-election-and-stability-without-eventual-timely-links/).

### How do I get set up? ###

* gradle compileJava
* gradle test

#### Expected test failures ####
* FLEV2CombValidLeaderElectTest - combinatorial tests take time.
* FLECompareFLEV2Test - expected failures in old leader election.
* NIO code needs TLC won't be committed to ZK.
