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

import java.util.BitSet;

/**
 * Provides connectivity mesh configuration control and query.
 * Remember connectivity is commutative.
 */
public interface QuorumCnxMesh {
    /**
     * How many entities are configured.
     * @return the size of the configured quorum.
     */
    int size();

    /**
     * Disconnect all from this sid.
     * @param sid
     */
    void disconnectAll(final long sid);

    /**
     * Connect all from this sid.
     * @param sid
     */
    void connectAll(final long sid);

    /**
     * Connect sid1 to sid2 and vice-versa
     * @param sid1
     * @param sid2
     */
    void connect(final long sid1, final long sid2);

    /**
     * Disconnect sid1 from sid2 and vice-versa
     * @param sid1
     * @param sid2
     */
    void disconnect(final long sid1, final long sid2);

    /**
     * Or connectivity with the given connectivity set.
     * Throws runtime exception if there is a size mismatch.
     * @param sid
     * @param set
     */
    void orToSet(final long sid, final BitSet set);

    /**
     * Check if sid has any connectivity other than to itself of-course.
     * @param sid
     * @return
     */
    boolean isConnectedToAny(final long sid);

    /**
     * Check if there exists a connection between the pair of sids.
     * The connectivity check is bi-directional.
     * @param sid1
     * @param sid2
     * @return
     */
    boolean connected(final long sid1, final long sid2);
}
