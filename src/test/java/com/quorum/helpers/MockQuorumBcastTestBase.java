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

import com.common.X509Exception;
import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;


public class MockQuorumBcastTestBase extends AbstractQuorumBcastTestWrapper {
    public MockQuorumBcastTestBase(long id, int quorumSize) {
        super(id, quorumSize);
    }

    @Override
    public void addServer(QuorumServer server) throws ChannelException {}

    @Override
    public void removeServer(QuorumServer server) throws ChannelException {}

    @Override
    public void start(Callback<Vote> msgRxCb, String arg1,
                      String arg2, String arg3, String arg4,
                      String arg5)
            throws IOException, ChannelException, CertificateException, NoSuchAlgorithmException, X509Exception.KeyManagerException, X509Exception.TrustManagerException {}

    @Override
    public void broadcast(Vote vote) {}

    @Override
    public void shutdown() {}

    @Override
    public void runNow() {}
}
