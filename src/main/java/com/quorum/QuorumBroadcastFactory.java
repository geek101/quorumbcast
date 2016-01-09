/**
 *  Licensed to the Apache Software Foundation (ASF) under one
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

package com.quorum;

import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import io.netty.channel.EventLoopGroup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class QuorumBroadcastFactory {

    public static QuorumBroadcast createQuorumBroadcast(
            final String type, final Long myId,
            final List<QuorumServer> quorumServerList,
            final InetSocketAddress electionAddr,
            final EventLoopGroup eventLoopGroup,
            final long readTimeoutMsec, final long connectTimeoutMsec,
            final long keepAliveTimeoutMsec, final int keepAliveCount)
            throws ChannelException, IOException {
        if (type == null || type.equalsIgnoreCase("nio")) {
            return new com.quorum.nio.QuorumBroadcast(
                    myId, quorumServerList, electionAddr, eventLoopGroup);
        } else if (type.equalsIgnoreCase("netty")) {
            return new com.quorum.netty.QuorumBroadcast(
                    myId, quorumServerList, electionAddr, eventLoopGroup,
                    readTimeoutMsec, connectTimeoutMsec,
                    keepAliveTimeoutMsec, keepAliveCount, false);
        } else if (type.equalsIgnoreCase("netty-ssl")) {
            return new com.quorum.netty.QuorumBroadcast(
                    myId, quorumServerList, electionAddr, eventLoopGroup,
                    readTimeoutMsec, connectTimeoutMsec, keepAliveTimeoutMsec,
                    keepAliveCount, true);
        } else {
            throw new RuntimeException("Invalid type: " + type);
        }
    }
}
