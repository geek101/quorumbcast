package com.quorum.nio;

import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.util.ChannelException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by powell on 11/17/15.
 */
public abstract class QuorumBroadcastImpl implements com.quorum.QuorumBroadcast {
    private final Long myId;
    private final InetSocketAddress electionAddr;

    protected Map<Long, QuorumServer> serverMap = new HashMap<>();

    private Vote voteToSend = null;

    public QuorumBroadcastImpl(Long myId, List<QuorumServer> quorumServerList,
                           InetSocketAddress electionAddr)
            throws ChannelException, IOException {
        this.myId = myId;
        this.electionAddr = electionAddr;
        for (QuorumServer server : quorumServerList) {
            addServerImpl(server);
        }
    }

    public long sid() {
        return myId;
    }

    /**
     * API to add a server.
     * @param server
     * @throws ChannelException
     */
    public abstract void addServer(final QuorumServer server)
            throws ChannelException;

    /**
     * API to remove a server
     * @param server
     * @throws ChannelException
     */
    public abstract  void removeServer(final QuorumServer server)
            throws ChannelException;


    protected final InetSocketAddress getElectionAddr() { return electionAddr; }

    /**
     * API to broadcast the given message.
     * This is not a queue service. If a previous outstanding
     * message (i.e message could not be sent etc) exists it will
     * not be sent any more and will be replaced by the new message.
     * @param msg
     */
    public void broadcast(Vote msg) {
        setSendMsg(msg);
    }

    synchronized protected void setSendMsg(Vote vote) {
        voteToSend = vote;
    }

    synchronized protected Vote getSendMsg() {
        return voteToSend;
    }

    protected void addServerImpl(final QuorumServer server)
            throws ChannelException {
        if (serverMap.containsKey(server.id())) {
            throw new ChannelException("Invalid config, server key:"
                    + server.id() + "collision: server1: "
                    + server.toString() + " server2: " + serverMap.get
                    (server.id()));
        }
        serverMap.put(server.id(), server);
    }

    protected QuorumServer getServer(long sid) {
        return serverMap.get(sid);
    }
}
