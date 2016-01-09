/**
 *  Licensed to the Apache Software Foundation (ASF) under one
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

import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by powell on 11/8/15.
 * This class absctracts away selector details, and needs
 * help from user to schedule all channels. Supports only one server socket.
 */
public class SelectorDispatch {
    private static final Logger LOG =
            LoggerFactory.getLogger(SelectorDispatch.class);
    final private InetSocketAddress listenAddr;  /// Via config
    final private long timeout_ms;               /// Via config, milliseconds
    private ServerSocketChannel ssc;             /// Server socket
    private Selector selector;                   /// Selector used
    private Map<SelectionKey, MsgChannel> channelMap
            = new HashMap<>();
    private Callback acceptHandler = null;   /// Call on accept ready

    public SelectorDispatch(final InetSocketAddress listenAddr,
                            long timeout_ms)
            throws IOException {
        this.listenAddr = listenAddr;
        this.timeout_ms = timeout_ms;
        selector = Selector.open();
        ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        ssc.socket().bind(listenAddr);
        ssc.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void close() throws IOException {
        ssc.close();
        ssc = null;
        selector.close();
        selector = null;
    }

    public void registerAccept(final Callback<SocketChannel> cb) {
        acceptHandler = cb;
    }

    /**
     * Called after connect is issued.
     * @param mc
     * @return
     * @throws IOException
     */
    public MsgChannel registerConnect(MsgChannel mc)
            throws IOException {
        SelectionKey key = mc.getChannel().register(selector,
                SelectionKey.OP_CONNECT);
        putKey(key, mc);
        return mc;
    }

    public MsgChannel registerRead(MsgChannel mc) throws IOException {
        SelectionKey key = mc.getChannel().register(selector,
                SelectionKey.OP_READ);
        putKey(key, mc);
        return mc;
    }

    public MsgChannel registerWrite(MsgChannel mc) throws IOException {
        SelectionKey key = mc.getChannel().register(selector,
                SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        putKey(key, mc);
        return mc;
    }

    public void runNow() throws ChannelException, IOException {
        processRun(selectRunNow());
    }

    public void run() throws ChannelException, IOException {
        processRun(selectRun());
    }

    private int selectRunNow() throws IOException {
        return selector.selectNow();
    }

    private int selectRun() throws IOException {
        return selector.select(timeout_ms);
    }

    private void processRun(int noOfKeys) throws ChannelException, IOException {
        LOG.debug("No of selected keys: " + noOfKeys);
        Set selectedKeys = selector.selectedKeys();
        Iterator it = selectedKeys.iterator();
        while (it.hasNext()) {
            SelectionKey key = (SelectionKey) it.next();
            it.remove();
            process(key);
        }
    }

    private MsgChannel getMsgChannel(SelectionKey key) {
        return channelMap.get(key);
    }

    private void putKey(SelectionKey key, MsgChannel ch) {
        channelMap.put(key, ch);
    }

    private void delKey(SelectionKey key) {
        key.cancel();
        channelMap.remove(key);
    }

    /**
     * Process this ready socket channel
     * @param key
     */
    private void process(SelectionKey key)
            throws ChannelException, IOException {
        if (!key.isValid()) {
            delKey(key);
        } else if (key.isAcceptable()) {
            SocketChannel sc = ssc.accept();
            acceptHandler.call(sc);
            return;
        } else if (key.isConnectable() ||
                key.isReadable() || key.isWritable()) {
            MsgChannel mc = getMsgChannel(key);
            int op = key.interestOps();
            //delKey(key);  // remove they key from the set.
            mc.run(op);   // may or may not add the channel back.
        }
    }

    public Selector getSelector() {
        return selector;
    }
}
