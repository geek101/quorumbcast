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

package com.quorum.nio;

import com.quorum.util.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Base Msg channel, abstracts the socket channel and used by
 * the selector class which calls its run method when the
 * channel is on Connect/Read/Write ready
 * Created by powell on 11/9/15.
 */
public abstract class MsgChannel {
    private static final Logger LOG =
            LoggerFactory.getLogger(MsgChannel.class);
    private SocketChannel sc;            /// Channel to this server
                                          // from/to us.
    private IPipeline<MsgChannel> readerPipeline = null;  /// Reader pipeline.
    private MsgChannelCallable writerCallback = null;     /// What to do.


    public MsgChannel() throws IOException {
        this.sc = SocketChannel.open();
        setSockOpts();
    }

    public MsgChannel(SocketChannel sc) throws IOException {
        this.sc = sc;
        setSockOpts();
    }

    public SocketChannel getChannel() {
        return sc;
    }

    /**
     * Unique key helper.
     * @return
     */
    public abstract long getKey();

    public abstract void close();

    protected void closeInt() throws IOException {
        readerPipeline = null;
        writerCallback = null;
        if (sc != null) {
            if (sc.isOpen()) {
                sc.close();
            }
            sc = null;
        }
    }

    /**
     * Implement what to do for connected event
     */
    protected abstract void connected() throws ChannelException, IOException;

    /**
     * Select driver makes this call. Supports multiple ops here
     * Process connect first, write next and read.
     * @throws Exception
     */
    public void run(int op) throws ChannelException, IOException {
        try {
            if ((op & SelectionKey.OP_CONNECT) != 0) {
                connected();
            }

            if ((op & SelectionKey.OP_WRITE) != 0
                     && writerCallback != null) {
                writerCallback.execute(this);
            }

            if ((op & SelectionKey.OP_READ) != 0
                    && readerPipeline != null) {
                readerPipeline.runNext(this);
            }
        } catch (ChannelException | IOException exp) {
            String addr = getChannel().toString();
            LOG.warn("Channel: " + addr + " error: " + exp);
            close();
        }
    }

    public void registerReaderPipeline(
            IPipeline<MsgChannel> readerPipeline) {
        this.readerPipeline = readerPipeline;
    }

    public void registerWriterCb(MsgChannelCallable callback) {
        this.writerCallback = callback;
    }

    protected boolean isWriterBusy() {
        return this.writerCallback != null;
    }

    protected Boolean connect(InetSocketAddress server) throws IOException {
        return sc.connect(server);
    }

    protected Boolean finishConnect() throws IOException {
        return sc.finishConnect();
    }

    public int read(ByteBuffer buf) throws IOException {
        return sc.read(buf);
    }

    protected int write(ByteBuffer buf) throws IOException {
        return sc.write(buf);
    }

    private void setSockOpts() throws IOException {
        sc.configureBlocking(false);
        sc.setOption(StandardSocketOptions.TCP_NODELAY,true);
        sc.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
    }
}
