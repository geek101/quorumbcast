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

import com.quorum.QuorumBroadcast;
import com.quorum.VoteView;
import io.netty.channel.EventLoopGroup;

import java.net.InetSocketAddress;

/**
 * Do not add any methods or override any methods from base class. We
 * need to test the base class as is except for this.
 */
public class VoteViewWrapper extends VoteView {
    /**
     * Used for testing.
     * @param mySid
     * @param electionAddr
     * @param quorumBroadcast
     */
    protected VoteViewWrapper(final Long mySid,
                              final InetSocketAddress electionAddr,
                              final EventLoopGroup eventLoopGroup,
                              final QuorumBroadcast quorumBroadcast) {
        super(mySid, electionAddr, eventLoopGroup, quorumBroadcast);
    }
}
