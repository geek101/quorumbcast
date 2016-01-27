package com.quorum;

public class QuorumPeer {
    public enum LearnerType {
        PARTICIPANT, OBSERVER;
    }

    public enum ServerState {
        LOOKING, FOLLOWING, LEADING, OBSERVING;
    }
}
