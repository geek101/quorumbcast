/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quorum;

import com.quorum.helpers.VoteViewTopo;
import com.quorum.netty.BaseTest;
import com.quorum.util.ChannelException;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class VoteViewTest extends BaseTest {
    private static final Logger LOG
            = LoggerFactory.getLogger(VoteViewTest.class);
    private final String type;
    private final Integer portStart;
    private final Integer maxPeerCount;
    private final Long readTimeoutMsec;
    private final Long connectTimeoutMsec;
    private final Long keepAliveTimeoutMsec;
    private final Integer keepAliveCount;
    private final Long sidStart;
    private final Long sidEnd;
    private VoteViewTopo voteViewTopo;

    @Parameterized.Parameters
    public static Collection quorumServerConfigs() {
        return Arrays.asList( new Object [][] {
                // SSL, ListenPortStart, Num of Views, ReadTimeout,
                // ConnectTimeout, KeepAliveTimeout, KeepAliveCount, StartSid,
                // EndSid
                //{ "nio", 11111, 3, 0L, 0L, 0L, 0, 1L, 99L},
                { "netty-ssl", 12222, 3, 0L, 0L, 0L, 0, 100L, 199L},
                { "netty",  13333, 15, 500L, 10L, 200L, 3, 200L, 299L},
                { "netty-ssl", 14444, 13, 2500L, 1000L, 200L, 3, 300L, 399L},
        });
    }

    public VoteViewTest(final String type, final int portStart,
                        final int maxPeerCount, final long readTimeoutMsec,
                        final long connectTimeoutMsec,
                        final long keepAliveTimeoutMsec,
                        final int keepAliveCount, long sidStart, long sidEnd)
            throws ChannelException {
        this.type = type;
        this.portStart = portStart;
        this.maxPeerCount = maxPeerCount;
        this.readTimeoutMsec = readTimeoutMsec;
        this.connectTimeoutMsec = connectTimeoutMsec;
        this.keepAliveTimeoutMsec = keepAliveTimeoutMsec;
        this.keepAliveCount = keepAliveCount;
        this.sidStart = sidStart;
        this.sidEnd = sidEnd;
    }

    @Before
    public void setup() throws Exception {
        LOG.info("Setup type: " + type);
        ClassLoader cl = getClass().getClassLoader();
        if (type.equals("netty")|| type.equals("nio")) {
            sslEnabled = false;
        } else if (type.equals("netty-ssl")) {
            sslEnabled = true;
        } else {
            throw new IllegalArgumentException("type is invalid:" + type);
        }

        voteViewTopo = new VoteViewTopo(type, sslEnabled, this.portStart, this
                .maxPeerCount, this.readTimeoutMsec, this.connectTimeoutMsec,
                this.keepAliveTimeoutMsec, this.keepAliveCount,
                this.sidStart, this.sidEnd,
                cl.getResource(keyStore.get(0)).getFile(),
                keyPassword.get(0),
                cl.getResource(trustStore.get(0)).getFile(),
                trustPassword.get(0),
                trustStoreCAAlias);
    }

    @Test
    public void test1TwoAndAddRest() throws Exception {
        twoPeerViewHelper();

        // Add rest of the peers.
        voteViewTopo.buildView(maxPeerCount - 2);
        voteViewTopo.setServers();

        // Lets make sure new Views got current votes.
        voteViewTopo.waitForVotes();
        assertTrue(voteViewTopo.voteConverged());

        // Now set votes for new Views
        voteViewTopo.setVotes();

        // Verify full convergence
        voteViewTopo.waitForVotes();
        assertTrue(voteViewTopo.voteConverged());
    }

    private void twoPeerViewHelper() throws Exception {
        voteViewTopo.buildView(2);
        voteViewTopo.setServers();
        voteViewTopo.resetVotesAll();
        voteViewTopo.waitForVotes();
        assertTrue(voteViewTopo.voteConverged());
    }
}
