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

package com.quorum.nio.msghelper;

import com.quorum.util.ChannelException;

import java.io.IOException;

public class ReadMsgLen extends ReadNumber<Integer, Integer> {
    public static final int PACKETMAXSIZE = 1024 * 512;
    private static Integer t = Integer.MAX_VALUE;
    public ReadMsgLen() throws ChannelException, IOException {
        super(t);
    }

    public void ctxPreRead(Object ctx) {}

    public void ctxPostRead(Object ctx) throws ChannelException, IOException {
        Integer length = getBuf();
        if (length <= 0 || length > PACKETMAXSIZE) {
            throw new IOException(
                    "Received packet with invalid packet: "
                            + length);
        }
    }

    @Override
    public Integer getResult() {
        return getBuf();
    }
}
