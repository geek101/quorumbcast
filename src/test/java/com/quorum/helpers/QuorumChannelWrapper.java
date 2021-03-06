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

package com.quorum.helpers;

import com.quorum.QuorumServer;
import com.quorum.nio.SelectorDispatch;
import com.quorum.nio.VotingChannel;
import com.quorum.util.ChannelException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Wrapper to help testing VotingChannel, exports more
 * methods to poker around the internal data. Should not
 * impact its behaviour.
 * Created by powell on 11/15/15.
 */
public class QuorumChannelWrapper extends VotingChannel {
    public QuorumChannelWrapper(Long myId,
                                InetSocketAddress electionAddr,
                                QuorumServer server,
                                SelectorDispatch selector)
            throws ChannelException, IOException {
        super(myId, electionAddr, server, selector);
    }

    public QuorumChannelWrapper(Long myId,
                                InetSocketAddress electionAddr,
                                SocketChannel sc, SelectorDispatch selector)
            throws ChannelException, IOException {
        super(myId, electionAddr, sc, selector);
    }

    public boolean isConnecting() {
        return stateConnecting() && getChannel()
                .isConnectionPending();
    }

    public boolean stateConnecting() {
        return state() == State.CONNECTING;
    }

    public boolean stateConnected() {
        return state() == State.READWRITE;
    }

    public final QuorumServer getConnectedQuorumServer() {
        return connectedServer;
    }

    public long writeMsgCount() {
        return writeMsgCount;
    }
}
