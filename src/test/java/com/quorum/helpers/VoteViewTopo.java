package com.quorum.helpers;

import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.VoteView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class VoteViewTopo {
    private static final Logger LOG
            = LoggerFactory.getLogger(VoteViewTopo.class);
    private final String type;
    private final Integer portStart;
    private final Integer maxPeerCount;
    private final Long readTimeoutMsec;
    private final Long connectTimeoutMsec;
    private final Long keepAliveTimeoutMsec;
    private final Integer keepAliveCount;
    private final Long sidStart;
    private final Long sidEnd;
    private final Boolean sslEnabled;

    private final String keyStore;
    private final String keyStorePassword;
    private final String trustStore;
    private final String trustStorePassword;
    private final String trustStoreCAAlias;

    private Long sidCurrent = null;

    private final Map<Long, VoteView> voteViewMap = new HashMap<>();
    private final Map<Long, List<QuorumServer>> serverViewMap = new HashMap<>();
    private final Map<Long, Vote> voteMap = new HashMap<>();

    final long rgenseed = System.currentTimeMillis();
    Random random = new Random(rgenseed);

    public VoteViewTopo(final String type, final boolean sslEnabled,
                        final int portStart, final int maxPeerCount,
                        final long readTimeoutMsec,
                        final long connectTimeoutMsec,
                        final long keepAliveTimeoutMsec,
                        final int keepAliveCount,
                        long sidStart, long sidEnd, final String keyStore,
                        final String keyStorePassword,
                        final String trustStore,
                        final String trustStorePassword,
                        final String trustStoreCAAlias) {
        this.type = type;
        this.sslEnabled = sslEnabled;
        this.portStart = portStart;
        this.maxPeerCount = maxPeerCount;
        this.readTimeoutMsec = readTimeoutMsec;
        this.connectTimeoutMsec = connectTimeoutMsec;
        this.keepAliveTimeoutMsec = keepAliveTimeoutMsec;
        this.keepAliveCount = keepAliveCount;
        this.sidStart = sidStart;
        this.sidEnd = sidEnd;

        this.keyStore = keyStore;
        this.keyStorePassword = keyStorePassword;
        this.trustStore = trustStore;
        this.trustStorePassword = trustStorePassword;
        this.trustStoreCAAlias = trustStoreCAAlias;

        this.sidCurrent = this.sidStart;
    }

    /**
     * Builds n VoteViews
     * @param count
     */
    public void buildView(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            addVoteView();
        }
    }

    /**
     * Call to make every View have the same servers.
     */
    public void setServers() throws Exception {
        final List<QuorumServer> serverList = new ArrayList<>();
        for (VoteView view : voteViewMap.values()) {
            serverList.add(view.getServer());
        }

        for (VoteView view : voteViewMap.values()) {
            for (QuorumServer server : serverList) {
                if (setServer(view, server)) {
                    view.addServer(server);
                }
            }
        }
    }

    public void resetVotesAll() throws InterruptedException,
            ExecutionException {
        for (VoteView view : voteViewMap.values()) {
            Vote vote = new Vote(random.nextLong(), random.nextLong(),
                    view.getId());
            voteMap.put(vote.getSid(), vote);
            view.updateSelfVote(vote);
        }
    }

    public void setVotes() throws InterruptedException, ExecutionException {
        for (VoteView view : voteViewMap.values()) {
            // if no vote set it.
            if (!voteMap.containsKey(view.getId())) {
                Vote vote = new Vote(random.nextLong(), random.nextLong(),
                        view.getId());
                voteMap.put(vote.getSid(), vote);
                view.updateSelfVote(vote);
            }
        }
    }

    public void waitForVotes() throws InterruptedException {
        int max_count = voteMap.values().size(); // source of truth
        int max_votes_checked = 0;
        while (max_votes_checked < voteViewMap.values().size()) {
            max_votes_checked = 0;
            for (VoteView view : voteViewMap.values()) {
                if (view.getVotes().size() == max_count) {
                    max_votes_checked++;
                }
            }
            Thread.sleep(1);
        }
    }

    /**
     * Must be called after waitForVotes()
     * @return
     */
    public boolean voteConverged() {
        Collection<Vote> votes = voteMap.values(); // src of truth
        for (VoteView view : voteViewMap.values()) {
            if (!voteViewEquals(view.getVotes(), votes)) {
                return false;
            }
        }

        return true;
    }

    private boolean setServer(final VoteView voteView,
                              final QuorumServer server) {
        if (voteView.getId() == server.id()) {
            return false;
        }

        if (serverViewMap.get(voteView.getId()) == null) {
            serverViewMap.put(voteView.getId(), new LinkedList<QuorumServer>());
        }

        for (final QuorumServer s : serverViewMap.get(voteView.getId())) {
            if (s.id() == server.id()) {
                return false;
            }
        }

        serverViewMap.get(voteView.getId()).add(server);
        return true;
    }

    private boolean removeServer(final VoteView voteView,
                                 final QuorumServer server) {
        if (voteView.getId() == server.id()) {
            return false;
        }

        Iterator<QuorumServer> it
                = serverViewMap.get(voteView.getId()).iterator();
        while(it.hasNext()) {
            if (it.next().id() == server.id()) {
                it.remove();
                return true;
            }
        }

        return false;
    }

    private boolean voteViewEquals(final Collection<Vote> map1,
                                   final Collection<Vote> map2) {
        final Map<Long, Vote> hashMap1 = new HashMap<>();
        final Map<Long, Vote> hashMap2 = new HashMap<>();

        if (map1.size() != map2.size()) {
            LOG.error("Vote maps size failed, map1: " + map1.size()
                    + " map2: " + map2.size());
            return false;
        }

        for (Vote vote : map1) {
            if (hashMap1.containsKey(vote.getSid())) {
                LOG.error("duplicate vote: " + vote);
                return false;
            }
            hashMap1.put(vote.getSid(), vote);
        }

        for (Vote vote : map2) {
            if (hashMap2.containsKey(vote.getSid())) {
                LOG.error("duplicate vote: " + vote);
                return false;
            }
            hashMap2.put(vote.getSid(), vote);
        }

        Iterator<Vote> it = hashMap2.values().iterator();
        while(it.hasNext()) {
            Vote vote2 = it.next();
            if (!hashMap1.containsKey(vote2.getSid()) ||
                    !hashMap1.get(vote2.getSid()).match(vote2)) {
                LOG.error("Could not match vote: " + vote2);
                return false;
            }
        }

        return true;
    }
    /**
     * helper to initialize each VoteView, starts with empty server list.
     * @throws Exception
     */
    private void addVoteView() throws Exception {
        VoteView voteView;
        if (voteViewMap.size() < maxPeerCount) {
            Long sid = this.sidCurrent++;
            InetSocketAddress electionAddr = new InetSocketAddress("localhost",
                    portStart+(int)(sid-this.sidStart));

            voteView = new VoteView(type, sid, new ArrayList<QuorumServer>(),
                        electionAddr, readTimeoutMsec, connectTimeoutMsec,
                    keepAliveTimeoutMsec, keepAliveCount);
        } else {
            throw new IllegalAccessException("Max peer count used: "
                    + voteViewMap.size());
        }

        voteView.start(keyStore, keyStorePassword, trustStore,
                trustStorePassword, trustStoreCAAlias);
        voteViewMap.put(voteView.getId(), voteView);
    }

    private QuorumServer removeVoteView(final Long sid) throws Exception {
        if (!voteViewMap.containsKey(sid)) {
            throw new IllegalArgumentException("invalid sid: " + sid);
        }

        final QuorumServer server = voteViewMap.get(sid).getServer();
        voteViewMap.get(sid).shutdown();
        voteViewMap.remove(sid);
        return server;
    }

    private VoteView resetVoteView(final QuorumServer server)
            throws Exception {
        ClassLoader cl = getClass().getClassLoader();
        VoteView voteView = voteViewMap.get(server.id());
        if (voteView != null) {
            removeVoteView(server.id());
        }

        voteView = new VoteView(type, server.id(), new
                    ArrayList<QuorumServer>(), server.getElectionAddr(),
                    readTimeoutMsec, connectTimeoutMsec, keepAliveTimeoutMsec,
                keepAliveCount);

        voteView.start(keyStore, keyStorePassword, trustStore,
                trustStorePassword, trustStoreCAAlias);
        voteViewMap.put(server.id(), voteView);
        return voteView;
    }
}