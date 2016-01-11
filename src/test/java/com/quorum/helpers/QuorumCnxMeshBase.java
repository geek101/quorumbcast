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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class QuorumCnxMeshBase implements QuorumCnxMesh {
    private static final Logger LOG
            = LoggerFactory.getLogger(QuorumCnxMeshBase.class);
    final int quorumSize;
    final Map<Long, BitSet> meshBitSet;

    public QuorumCnxMeshBase(final int quorumSize) {
        this.quorumSize = quorumSize;
        this.meshBitSet = new HashMap<>();
        for (long i = 1; i <= (long)this.quorumSize; i++) {
            final BitSet bset = new BitSet(quorumSize+1);
            meshBitSet.put(i, bset);
            // connect to self.
            connect(i, i);
        }
    }

    @Override
    public int size() {
        return quorumSize;
    }

    @Override
    public void disconnectAll(final long sid) {
        synchronized (this) {
            if (!meshBitSet.containsKey(sid)) {
                final String errStr = "invalid sid for bitset : " + sid
                        + ", quorum size: " + quorumSize;
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }
            for (final long targetSid : meshBitSet.keySet()) {
                if (sid != targetSid) {
                    disconnect(sid, targetSid);
                }
            }
        }
    }

    @Override
    public void connectAll(final long sid) {
        synchronized (this) {
            if (!meshBitSet.containsKey(sid)) {
                final String errStr = "invalid sid for bitset : " + sid
                        + ", quorum size: " + quorumSize;
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }
            for (final long targetSid : meshBitSet.keySet()) {
                if (sid != targetSid) {
                    connect(sid, targetSid);
                }
            }
        }
    }

    @Override
    public void connect(final long sid1, final long sid2) {
        setUnSetHelper(sid1, sid2, true);
    }

    @Override
    public void disconnect(final long sid1, final long sid2) {
        setUnSetHelper(sid1, sid2, false);
    }

    @Override
    public boolean isConnectedToAny(final long sid) {
        synchronized (this) {
            if (!meshBitSet.containsKey(sid)) {
                final String errStr = "invalid sid for bitset : " + sid
                        + ", quorum size: " + quorumSize;
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }

            final BitSet copy = (BitSet) meshBitSet.get(sid).clone();
            copy.set((int) sid, false);
            return !copy.isEmpty();
        }
    }

    @Override
    public void orToSet(final long sid, final BitSet set) {
        synchronized (this) {
            if (set.size() != meshBitSet.get(sid).size()) {
                final String errStr = "invalid bitset with size: " + set.size()
                        + ", expected size: " + meshBitSet.get(sid).size();
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }
            meshBitSet.get(sid).or(set);
        }
    }

    @Override
    public boolean connected(final long sid1, final long sid2) {
        synchronized (this) {
            return meshBitSet.get(sid1).get((int) sid2) &&
                    meshBitSet.get(sid2).get((int) sid1);
        }
    }

    private void setUnSetHelper(final long sid1, final long sid2,
                                final boolean pred) {
        synchronized (this) {
            if (!meshBitSet.containsKey(sid1) ||
                    !meshBitSet.containsKey(sid2)) {
                final String errStr = "invalid sids for bitset : " + sid1
                        + ", " + sid2 + ", quorum size: " + quorumSize;
                LOG.error(errStr);
                throw new IllegalArgumentException(errStr);
            }
            meshBitSet.get(sid1).set((int) sid2, pred);
            meshBitSet.get(sid2).set((int) sid1, pred);
        }
    }
}
