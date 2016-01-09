package com.quorum;

/**
 * Created by powell on 12/21/15.
 */
public class QuorumPeer {
    public enum LearnerType {
        PARTICIPANT, OBSERVER;
    }

    public enum ServerState {
        LOOKING, FOLLOWING, LEADING, OBSERVING;
    }
}
