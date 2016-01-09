package com.quorum;

public interface VoteViewRead {
    /**
     * API to get the current Vote of the system.
     * @return If Vote exists else it will be null.
     */
    Vote getSelfVote();
}
