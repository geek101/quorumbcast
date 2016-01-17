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
 * Helps with reading when data is size of a Number, aka Short/Integer/Long etc
 */
public abstract class ReadNumber<T extends Number, R extends Object> implements
        ReadMsgCallback<R> {
    private final int size;
    private final T obj;           /// Must be a better way!

    private ByteBuffer buf;
    private boolean firstRead = false;
    private boolean lastRead = false;
    private int readSize = 0;
    private boolean retry = true;
    private T result = null;

    public ReadNumber(T obj)
            throws ChannelException, IOException {
        this.obj = obj;
        if (this.obj instanceof Integer) {
            this.size = Integer.BYTES;
        } else if (this.obj instanceof Long) {
            this.size = Long.BYTES;
        } else if (this.obj instanceof Short) {
            this.size = Short.BYTES;
        } else {
            throw new ChannelException("Invalid type ");
        }
        buf = ByteBuffer.allocate(this.size);
    }

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

        if (!buf.hasRemaining()) {
            setResult();
            return;
        }

        long ret = ch.read(buf);
        readSize += ret;

        if (readSize == size) {
            setResult();
            if (!lastRead) {
                lastRead = true;
                ctxPostRead(ctx);
            }
            return;
        }
    }

    public boolean shouldRetry() {
        return retry;
    }

    public T getBuf() {
        return result;
    }

    public abstract void ctxPreRead(Object ctx);
    public abstract void ctxPostRead(Object ctx)
            throws ChannelException, IOException;

    protected void resetResult(T o) {
        result = o;
    }

    @SuppressWarnings("unchecked")
    private void setResult() throws ChannelException {
        buf.flip();
        if (obj instanceof Integer) {
            result = (T)new Integer(buf.getInt());
        } else if (obj instanceof Long) {
            result = (T)new Long(buf.getLong());
        } else if (obj instanceof Short) {
            result = (T)new Short(buf.getShort());
        } else {
            throw new ChannelException("Invalid type ");
        }

        buf = null;
        retry = false;
    }
}
