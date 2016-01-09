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

import com.quorum.nio.MsgChannel;
import com.quorum.util.InitMessageCtx;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Helps with testing VotingChannel.
 * Created by powell on 11/15/15.
 */
public class MockMsgChannel extends MsgChannel {
    private final Long key;
    private final InetSocketAddress serverAddr;

    /**
     * The state of the channel.
     */
    public enum State {
        ERROR, CONNECTING, CONNECTED, READWRITE,
    }

    private State state = State.ERROR;

    public MockMsgChannel(Long key,
                          InetSocketAddress serverAddr) throws
            IOException {
        super();
        this.key = key;
        this.serverAddr = serverAddr;
    }

    public MockMsgChannel(Long key, SocketChannel channel)
            throws IOException {
        super(channel);
        this.key  = key;
        this.serverAddr = null;
        state = State.CONNECTED;
    }

    public void close() {
        try {
            closeInt();
        } catch (IOException exp) {

        }
    }

    public long getKey() {
        return key;
    }

    /**
     * Non blocking connect.
     * @return
     * @throws IOException
     */
    public boolean connect() throws IOException {
        state = State.CONNECTING;
        return connect(serverAddr);
    }

    protected void connected() throws IOException {
        if (!finishConnect()) {
            close();
            return;
        }

        state = State.CONNECTED;
    }

    public final State getState() {
        return state;
    }

    /**
     * Helper to read the hdr to verify rx side,
     * will block till read and channel should not be
     * part of a selector so when we set it to non blocking
     * it does not block selector loop for ever.
     * @param timeout read timeout
     * @param serverAddr What it tx side addr used.
     * @return Buffer backing the header read.
     * @throws IOException
     */
    public ByteBuffer readHdrBlocking(int timeout,
                                      InetSocketAddress serverAddr)
            throws IOException {
        setBlocking();
        getChannel().socket().setSoTimeout(timeout);

        // Lets calc what size to read.
        // PROTOCOL_VERSION + SID + LEN + HOSTANDPORT_STRING
        int size = Long.SIZE/8 + Long.SIZE/8 + Integer.SIZE/8
                + InitMessageCtx.getAddrString(serverAddr).getBytes().length;

        ByteBuffer buf = ByteBuffer.allocate(size);
        long ret = 0;
        while (ret < size) {
            ret += getChannel().read(buf);
        }
        setNonBlocking();

        buf.flip();
        return buf;
    }

    /**
     * Helper to read the message received by this channel.
     * @return ByteBuffer only without the length header
     * @throws IOException
     */
    public ByteBuffer readMsgBlocking(int timeout) throws IOException {
        setBlocking();
        getChannel().socket().setSoTimeout(timeout);

        ByteBuffer hdr = ByteBuffer.allocate(Integer.SIZE/8);

        while (hdr.hasRemaining()) {
            getChannel().read(hdr);
        }

        hdr.flip();
        int size = hdr.getInt();
        ByteBuffer buf = ByteBuffer.allocate(size);
        while (buf.hasRemaining()) {
            getChannel().read(buf);
        }

        setNonBlocking();
        buf.flip();
        return buf;
    }

    public void send(ByteBuffer buf) throws IOException {
        while (buf.remaining() > 0) {
            write(buf);
        }
    }

    private void setBlocking() throws IOException {
        getChannel().configureBlocking(true);
    }

    private void setNonBlocking() throws IOException {
        getChannel().configureBlocking(false);
    }
}
