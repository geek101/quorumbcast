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

import com.quorum.nio.MsgChannel;
import com.quorum.util.ChannelException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Helper to read variable data at runtime.
 */
public abstract class ReadLen<T> implements ReadMsgCallback<T> {
    protected Integer size = null;

    protected ByteBuffer buf = null;
    private boolean firstRead = false;
    private boolean lastRead = false;
    private int readSize = 0;
    private boolean retry = true;

    public ReadLen() {}

    /**
     * Read till we have enough data
     * @param ctx Ignored here
     * @param ch Channel that wraps SocketChannel
     * @throws Exception
     */
    public void readMsg(Object ctx, MsgChannel ch)
            throws ChannelException, IOException {
        if (!firstRead) {
            firstRead = true;
            ctxPreRead(ctx);
        }
        if (buf == null) {
            // ok lets allocate buf here
            if (size == null) {
                size = (Integer) ctx;
            }
            buf = ByteBuffer.allocate(size);
        }

        if (!buf.hasRemaining()) {
            return;
        }

        long ret = ch.read(buf);
        readSize += ret;

        if (readSize == size) {
            retry = false;
            buf.flip();
            if (!lastRead) {
                lastRead = true;
                ctxPostRead(ctx);
            }
        }
    }

    public boolean shouldRetry() {
        return retry;
    }

    public ByteBuffer getBuf() {
        return buf;
    }

    public abstract void ctxPreRead(Object ctx);
    public abstract void ctxPostRead(Object ctx)
            throws ChannelException, IOException;

    protected void setSize(Integer size) {
        this.size = size;
    }
}
