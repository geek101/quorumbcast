/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>http://www.apache.org/licenses/LICENSE-2.0</p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.quorum.netty;

import com.quorum.QuorumBroadcastFactory;
import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class QuorumBroadcastTest extends BaseTest {
    private static final Logger LOG
            = LoggerFactory.getLogger(QuorumBroadcastTest.class);

    private final String type;
    private final long readTimeoutMsec;
    private final long connectTimeoutMsec;
    private final long keepAliveTimeoutMsec;
    private final int keepAliveCount;
    private HashMap<Long, QuorumServer> serverMap = new HashMap<>();
    private final ExecutorService executor
            = Executors.newSingleThreadExecutor();
    private EventLoopGroup eventLoopGroup;
    private HashMap<Long, List<QuorumServer>> qbastConfMap
            = new HashMap<>();
    private HashMap<Long, com.quorum.QuorumBroadcast> qbcastMap
            = new HashMap<>();
    private long idCount = 1L;
    final long rgenseed = System.currentTimeMillis();
    Random random = new Random(rgenseed);

    private final ConcurrentLinkedQueue<Vote> inboundVoteQueue
            = new ConcurrentLinkedQueue<>();

    private class MsgRxCb implements Callback<Vote> {
        @Override
        public void call(final Vote o) throws ChannelException, IOException {
            inboundVoteQueue.add(o);
        }
    }

    private final MsgRxCb msgRxCb = new MsgRxCb();

    @Before
    public void setup() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(1, executor);
        LOG.info("Setup type: " + type);
        ClassLoader cl = getClass().getClassLoader();
        int count = 0;
        for (Map.Entry<Long, List<QuorumServer>> entry :
                qbastConfMap.entrySet()) {
            com.quorum.QuorumBroadcast qbcast
                    = QuorumBroadcastFactory.createQuorumBroadcast(type,
                    entry.getKey(), entry.getValue(),
                    serverMap.get(entry.getKey()).getElectionAddr(), eventLoopGroup,
                    readTimeoutMsec, connectTimeoutMsec,
                    keepAliveTimeoutMsec, keepAliveCount);
            if (qbcast == null) {
                throw new NullPointerException("qbcast is invalid!, bailing");
            }

            qbcast.start(msgRxCb, cl.getResource(keyStore.get(count)).getFile(),
                        keyPassword.get(count),
                        cl.getResource(trustStore.get(0)).getFile(),
                        trustPassword.get(0),
                        trustStoreCAAlias);

            qbcastMap.put(entry.getKey(), qbcast);
            count++;
        }
    }

    @After
    public void tearDown() throws Exception {
        for (com.quorum.QuorumBroadcast qbcast : qbcastMap.values()) {
            qbcast.shutdown();
        }
        qbcastMap.clear();
        eventLoopGroup.shutdownGracefully().sync();
    }

    @SuppressWarnings("unchecked")
    public QuorumBroadcastTest(final String type,
                               final List<String> serverList,
                               final long readTimeoutMsec,
                               final long connectTimeoutMsec,
                               final long keepAliveTimeoutMsec,
                               final int keepAliveCount)
            throws ChannelException {
        this.type = type;
        try {
            for (String server : serverList) {
                serverMap.put(idCount,
                        new QuorumServer(idCount++, server));
            }
        } catch(ChannelException exp) {
            LOG.error("Config error: " + exp);
            throw exp;
        }

        for (Map.Entry<Long, QuorumServer> entry :
                serverMap.entrySet()) {
            Long id = entry.getKey();
            HashMap<Long, QuorumServer> mapCpy = (HashMap)serverMap.clone();
            mapCpy.remove(id);
            List<QuorumServer> list = new ArrayList<>();
            list.addAll(mapCpy.values());
            qbastConfMap.put(id, list);
        }

        this.readTimeoutMsec = readTimeoutMsec;
        this.connectTimeoutMsec = connectTimeoutMsec;
        this.keepAliveTimeoutMsec = keepAliveTimeoutMsec;
        this.keepAliveCount = keepAliveCount;
    }

    @Parameterized.Parameters
    public static Collection quorumServerConfigs() {
        return Arrays.asList( new Object [][] {
                //{ "nio", Arrays.asList("localhost:2888:15555",
                // "localhost:2888:16666") },
                { "netty-ssl", Arrays.asList("localhost:2888:25555",
                        "localhost:2888:26666"), 0, 0, 0L, 0 },
                { "netty-ssl", Arrays.asList("localhost:2888:25556",
                        "localhost:2888:26667"), 0, 10, 0L, 0 },
                { "netty-ssl", Arrays.asList("localhost:2888:25557",
                        "localhost:2888:26668"), 250, 100, 100, 3 },
                /*
                { "nio", Arrays.asList("localhost:2888:17777",
                "localhost:2888:18888",
                        "localhost:19999") },
                { "netty", Arrays.asList("localhost:2888:27777",
                        "localhost:2888:28888",
                        "localhost:2888:29999") },
                */
                /*
                { "nio", Arrays.asList("localhost:2888:20001",
                        "localhost:2888:21111",
                        "localhost:2888:22222", "localhost:2888:23333",
                        "localhost:2888:24444") },
                 */
        });
    }

    /**
     * Test a random sender and message being received by rest of them.
     */
    @SuppressWarnings("unchecked")
    @Test (timeout = 3500)
    public void testBroadcast() throws Exception {
        Vote v = new Vote (random.nextLong(), random.nextLong(),
                random.nextLong());

        LOG.info("Sending value: " + v.toString() + " Size: " + v.toString
                ().length());
        com.quorum.QuorumBroadcast sender = getSingleSender();
        HashMap<Long, com.quorum.QuorumBroadcast> rcvMap;
        rcvMap = (HashMap)qbcastMap.clone();
        rcvMap.remove(sender.sid());

        sender.broadcast(v);

        List<Vote> rxMsgs = new ArrayList<>();

        HashSet<Long> gotSet = new HashSet<>();
        while(rxMsgs.size() < rcvMap.size()) {
            sender.runNow();
            for (com.quorum.QuorumBroadcast rxQb : rcvMap.values()) {
                if (gotSet.contains(rxQb.sid())) {
                    continue;
                }

                // Only ones we have heard from yet.
                rxQb.runNow();
                Vote m = inboundVoteQueue.poll();
                if (m != null && m.equals(v)) {
                    rxMsgs.add(m);
                    gotSet.add(m.getSid());
                    break;
                }
            }
        }

        for (Vote rxMsg  : rxMsgs) {
            assertTrue(rxMsg.equals(v));
        }
    }

    @Test(timeout = 15000)
    public void runBroadcast10Times() throws Exception {
        for (int i = 0; i < 10; i++) {
            testBroadcast();
        }
    }

    private com.quorum.QuorumBroadcast getSingleSender() {
        Iterator<Map.Entry<Long, com.quorum.QuorumBroadcast>> iter
                = qbcastMap.entrySet().iterator();
        int rnd = random.nextInt(qbcastMap.values().size());
        for (int i = 0; i < rnd && iter.hasNext(); i++, iter.next());
        Map.Entry<Long, com.quorum.QuorumBroadcast> sendEntry = iter.next();
        return sendEntry.getValue();
    }
}
