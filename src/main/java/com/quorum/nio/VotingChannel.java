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

import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.nio.msghelper.*;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import com.quorum.util.InitMessageCtx;
import com.quorum.util.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * State for each server used by QuorumBcast.
 */
public class VotingChannel extends MsgChannel {
    private static final Logger LOG =
            LoggerFactory.getLogger(VotingChannel.class);

    private final Long myId;             /// What is our id?.
    private final InetSocketAddress electionAddr;  /// Our addr
    private final SelectorDispatch selector;   /// Who is our selector?.

    protected QuorumServer server = null;   /// Which server are we connected
    // to?.
    protected QuorumServer connectedServer = null;
    private Vote voteToSend = null;  /// Send only latest msg, not a queue
    private Vote fromMsg = null;   /// Incoming msg, keep latest.
    protected volatile long writeMsgCount = 0;
    protected volatile long readMsgCount = 0;

    /**
     * The state of the channel.
     */
    public enum State {
        ERROR, CONNECTING, READWRITE,
    }

    private State state = State.ERROR;

    private class WriteMsgCb implements MsgChannelCallable {
        private final ByteBuffer buf;
        private final Callback<MsgChannel> cb;
        public WriteMsgCb(ByteBuffer buf, Callback<MsgChannel> cb) {
            this.buf = buf;
            this.cb = cb;
        }

        public void execute(MsgChannel o)
                throws ChannelException, IOException {
            ((VotingChannel) o).writeMsg(buf, this);
        }

        public void done(MsgChannel o)
                throws ChannelException, IOException {
            cb.call(o);
        }
    }

    private class WriteHdrDone implements Callback<MsgChannel> {
        private final VotingChannel qc;
        public WriteHdrDone(VotingChannel qc) {
            this.qc = qc;
        }

        public void call(final MsgChannel o)
                throws ChannelException, IOException {
            this.qc.writeHdrDone();
        }
    }

    private class WriteMsgDone implements Callback<MsgChannel> {
        private final VotingChannel qc;
        public WriteMsgDone(VotingChannel qc) {
            this.qc = qc;
        }

        public void call(final MsgChannel o)
                throws ChannelException, IOException {
            this.qc.writeMsgDone();
        }
    }

    private class ReadVerDone implements Callback<Long> {
        private final VotingChannel qc;
        public ReadVerDone(VotingChannel qc) {
            this.qc = qc;
        }

        /**
         * Called after successful read of ProtocolVersion.
         * @param o Long value i.e the ProtocolVersion
         */
        public void call(final Long o)
                throws ChannelException, IOException {
            this.qc.gotProtocolVersion(o);
        }
    }

    private class ReadHdrDone implements Callback<InitMessageCtx> {
        private final VotingChannel qc;
        public ReadHdrDone(VotingChannel qc) {
            this.qc = qc;
        }

        /**
         * Called after successful read of Hdr.
         * @param o com.quorum.util.InitMessageCtx
         * @throws IOException
         */
        public void call(final InitMessageCtx o)
                throws ChannelException, IOException {
            this.qc.gotHdr(o);
        }
    }

    private class ReadMessageDone implements Callback<MessageCtx> {
        private final VotingChannel qc;
        public ReadMessageDone(VotingChannel qc) {
            this.qc = qc;
        }

        /**
         * Called after successful read of data i.e vote
         */
        public void call(MessageCtx o)
                throws ChannelException, IOException {
            this.qc.gotMessage(o);
        }
    }

    /**
     * Create a channel and connect to the server.
     * @param server
     */
    public VotingChannel(Long myId,
                         InetSocketAddress electionAddr,
                         QuorumServer server,
                         SelectorDispatch selector)
            throws ChannelException, IOException {
        super();
        this.myId = myId;
        this.electionAddr = electionAddr;
        this.server = server;
        this.selector = selector;

        try {
            Boolean ret = connect(server.getElectionAddr());
            if (!ret) {
                this.selector.registerConnect(this);
                setState(State.CONNECTING);
            } else {
                // connected, move to send hdr state
                sendHdr();
            }
        } catch (ChannelException | IOException exp) {
            LOG.error("Channel: " + getChannel().toString()
                    + " Error: " + exp);
            close();
        }
    }

    /**
     * Used for accepted connections.
     * @param myId Id of server where connection was accepted.
     * @param sc SocketChannel
     * @param selector
     * @throws ChannelException | IOException
     */
    public VotingChannel(Long myId, InetSocketAddress electionAddr,
                         SocketChannel sc, SelectorDispatch selector)
            throws ChannelException, IOException {
        super(sc);
        this.myId = myId;
        this.electionAddr = electionAddr;
        this.selector = selector;

        ReadMsgPipeline<Long> readVerPipeLine
                = new ReadMsgPipeline<>(new ReadVerDone(this), null);
        readVerPipeLine.add(new ReadVer());
        registerReaderPipeline(readVerPipeLine);
        selector.registerRead(this);
        setState(State.CONNECTING);
    }

    /**
     * API to read current message from remote server. Once read
     * it is cleared from here.
     * @return com.quorum.Message
     */
    public final Vote getMessage() {
        final Vote ret = fromMsg;
        fromMsg = null;
        return ret;
    }

    /**
     * API to write to this server. We try both channels if they are
     * available.
     * @param  vote What to send.
     * @throws IOException
     */
    public void sendMsg(Vote vote) {
        NotNull.check(vote, "sendMsg cannot handle null vote", LOG);

        // If we sent this message before then dont bother.
        if (voteToSend != null &&
                voteToSend.match(vote)) {
            return;
        }

        // If we are not ready to send data then store.
        if (isWriterBusy() || !getChannel().isConnected()) {
            return;
        }

        // Save here
        voteToSend = vote;

        // Build ByteBuffer
        // TODO: add API to Vote to get ByteBuffer directly?
        ByteBuffer msgToSend = voteToSend.buildMsgForNio();
        WriteMsgCb writeCb = new WriteMsgCb(msgToSend, new WriteMsgDone(this));
        try {
            write(msgToSend);
            if (msgToSend.hasRemaining()) {
                // we are done with this writer.
                registerWriterCb(writeCb);
                this.selector.registerWrite(this);
            } else {
                registerWriterCb(null);
                writeMsgDone();
            }
        } catch (ChannelException | IOException exp) {
            LOG.error("Channel: " + getChannel().toString()
                    + " Error: " + exp);
            close();
        }
    }

    /**
     * Returns Long.MIN_VALUE if key is unknown, which means this is
     * an incoming connection and we have to spend some time to learn
     * the key.
     * @return Long.MIN_VALUE for in-progress channel or sid when known.
     */
    public long getKey() {
        QuorumServer ret = server;
        if (ret == null) {
            ret = connectedServer;
        }
        if (ret == null) {
            return Long.MIN_VALUE;
        } else {
            return ret.id();
        }
    }

    public boolean closed() {
        return state() == State.ERROR;
    }

    @Override
    public void close() {
        try {
            closeInt();
        } catch (IOException exp) {
            // ignore this
        } finally {
            setState(State.ERROR);
        }
    }

    private void sendHdr() throws ChannelException, IOException {
        ByteBuffer msg = prepHdr();
        WriteMsgCb writeCb = new WriteMsgCb(msg, new WriteHdrDone(this));
        write(msg);
        if (msg.hasRemaining()) {
            registerWriterCb(writeCb);
            selector.registerWrite(this);
        } else {
            writeHdrDone();
        }
    }

    @SuppressWarnings("unchecked")
    private void registerForMessage() throws ChannelException, IOException {
        ReadMsgPipeline<MessageCtx> readMessage
                = new ReadMsgPipeline<>(new ReadMessageDone(this), null);
        readMessage.add(new ReadMsgLen())
                        .add(new ReadMessage());
        registerReaderPipeline(readMessage);
        selector.registerRead(this);
        setState(State.READWRITE);
    }

    /**
     * This channel had a connected event and it is in connecting state.
     */
    protected void connected() throws ChannelException, IOException {
        if (!finishConnect()) {
            close();
            return;
        }

        // send the hdr.
        setState(State.CONNECTING);
        sendHdr();
    }

    protected Selector getSelector() {
        return selector.getSelector();
    }

    /**
     * com.quorum.util.Callback used by com.quorum.nio.MsgChannel to write to other side
     * @msg Which message to send.
     * @throws IOException
     */
    private void writeMsg(ByteBuffer msg, MsgChannelCallable writeCb)
            throws ChannelException, IOException {
        write(msg);
        if (!msg.hasRemaining()) {
            // we are done with this writer.
            registerWriterCb(null);
            writeCb.done(this);
        } else {
            selector.registerWrite(this);
        }
    }

    /**
     * Reset the buffer with the hdr. Should be called before the
     * first call to sendHdr.
     */
    protected ByteBuffer prepHdr() {
        ByteBuffer buf = ByteBuffer.allocate(ReadRemainer.maxBuffer);
        final String electionAddrStr
                = InitMessageCtx.getAddrString(electionAddr);
        buf.putLong(InitMessageCtx.PROTOCOL_VERSION)
                .putLong(this.myId)
                .putInt(electionAddrStr.getBytes().length)
                .put(electionAddrStr.getBytes());
        buf.flip();
        return buf;
    }

    private void writeHdrDone() throws ChannelException, IOException {
        registerWriterCb(null);
        // move to read msg stage since this channel
        // survived.
        registerForMessage();
    }

    private void writeMsgDone() throws ChannelException, IOException {
        registerWriterCb(null);
        selector.registerRead(this);  /// Remove write interest.
        setState(State.READWRITE);
        writeMsgCount++;
    }

    /**
     * Called for an inbound channel, if did not survive challenge
     * then close the socket and let us be reaped.
     * @param sid The serverid.
     * @throws IOException
     */
    private boolean resolveSid(Long sid) throws IOException {
        if (sid < this.myId) {
            close();
            setState(State.ERROR);
            return false;
        } else {
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    private void gotProtocolVersion(Long protocolVersion)
            throws ChannelException, IOException {
        if (protocolVersion >= 0) {
            // we are done this is an older peer
            resolveSid(protocolVersion);
        } else {
            // read the full hdr
            InitMessageCtx initMsgCtx = new InitMessageCtx();
            initMsgCtx.protocolVersion = protocolVersion;
            ReadMsgPipeline<InitMessageCtx> readHdr = new ReadMsgPipeline<>(
                    new ReadHdrDone(this), initMsgCtx);
            readHdr.add(new ReadSid())
                    .add(new ReadRemainer())
                    .add(new ReadHostAndPort());
            registerReaderPipeline(readHdr);
            selector.registerRead(this);
        }
    }

    private void gotHdr(InitMessageCtx ctx)
            throws ChannelException, IOException {
        connectedServer = new QuorumServer(ctx.sid, ctx.addr);
        if (resolveSid(ctx.sid)) {
            // inbound survived moved to read com.quorum.Message state
            registerForMessage();
        }
    }

    private void gotMessage(MessageCtx ctx)
            throws ChannelException, IOException {
        if (server == null) {
            final String errStr = "Cannot get vote when server is unknown";
            LOG.error(errStr);
            throw new ChannelException(errStr);
        }
        fromMsg = Vote.buildVote(ctx.buf.remaining(), ctx.buf, server.id());
        readMsgCount++;

        // read next message.
        registerForMessage();
    }

    protected final State state() {
        return this.state;
    }

    private void setState(State state) {
        this.state = state;
    }
}
