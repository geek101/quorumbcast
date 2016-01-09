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
import com.quorum.util.InitMessageCtx;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Read the remainder from the server and parse it.
 * Created by powell on 11/11/15.
 */
public class ReadHostAndPort extends ReadLen {
    public ReadHostAndPort() {
        super();
    }

    public void ctxPreRead(Object ctx) {
        InitMessageCtx initMsgCtx = (InitMessageCtx) ctx;
        size = initMsgCtx.remainder;
        buf = ByteBuffer.allocate(size);
    }

    public void ctxPostRead(Object ctx)
            throws ChannelException, IOException {
        InitMessageCtx initMsgCtx = (InitMessageCtx) ctx;

        ByteBuffer buf = (ByteBuffer)getResult();
        byte[] b = new byte[initMsgCtx.remainder];
        buf.get(b);

        // FIXME: IPv6 is not supported. Using something like Guava's
        // HostAndPort parser would be good.
        String addr = new String(b);
        String[] host_port = addr.split(":");

        if (host_port.length != 2) {
            throw new ChannelException(
                    "Badly formed address: %s", addr);
        }

        int port;
        try {
            port = Integer.parseInt(host_port[1]);
        } catch (NumberFormatException e) {
            throw new ChannelException("Bad port number: %s",
                    host_port[1]);
        }

        initMsgCtx.addr = new InetSocketAddress(
                InetAddress.getByName(host_port[0]), port);
        resetResult(initMsgCtx);
    }
}
