package com.quorum.helpers;

import com.quorum.Vote;
import com.quorum.VoteViewBase;

import java.util.concurrent.Future;

public class VoteViewBaseWrapper extends VoteViewBase {
    public VoteViewBaseWrapper(final long mysid) {
        super(mysid);
    }
    public Future<Void> msgRxHelper(final Vote vote) {
        return super.msgRx(vote);
    }
}
