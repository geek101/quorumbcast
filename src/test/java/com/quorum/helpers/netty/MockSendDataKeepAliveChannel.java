package com.quorum.helpers.netty;


import com.quorum.Vote;
import com.quorum.util.ChannelException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertTrue;

/**
 * Created by powell on 12/27/15.
 */
public class MockSendDataKeepAliveChannel extends MockSendDataChannel {
    private ByteBuffer msgSent;
    private ByteBuffer msgRcv;
    public boolean keepAliveStart = false;
    public boolean keepAliveDisconnect = false;

    public MockSendDataKeepAliveChannel(final long key,
                                        final ByteBuffer hdrData,
                                        final ByteBuffer msgRcv,
                                        final long keepAliveTimeoutMsec,
                                        final int keepAliveCount) {
        super(key, hdrData, 0, keepAliveTimeoutMsec, keepAliveCount);
        this.msgRcv = msgRcv;
    }

    @Override
    public void sendMsg(ByteBuffer msg) throws ChannelException {

        msgSent = msg.duplicate();
        super.sendMsg(msg);
    }

    public void startKeepAliveTimer() throws ChannelException,
            InterruptedException {
        synchronized (this) {
            while(!keepAliveStart) {
                Thread.sleep(1);
            }
            resetKeepAliveTimer();
        }
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg,
                      final ChannelPromise promise) throws ChannelException {
        synchronized (this) {
            if (!keepAliveStart) {
                startKeepAliveTimer(ctx);
                keepAliveStart = true;
            }
        }
        ctx.writeAndFlush(msg, promise);
    }

    /**
     * Is called when a read timeout was detected. Override if required
     * but do not forget to call super().
     */
    protected void keepAliveTimeOut(ChannelHandlerContext ctx)
            throws ChannelException {
        super.keepAliveTimeOut(ctx);
        keepAliveDisconnect = true;
    }

    @Override
    protected void keepAliveSend(final ChannelHandlerContext ctx)
            throws ChannelException, IOException {
        LOG.info("keep alive send msg");
        sendMsg(msgSent.duplicate());
    }

    @Override
    protected void readMsg(final ChannelHandlerContext ctx,
                           final ByteBuf in) throws ChannelException {
        LOG.info("readmsg for keep alive channel");
        if (in.readableBytes() < msgRcv.remaining()) {
            return;
        }

        ByteBuffer readMsg = ByteBuffer.allocate(msgRcv.remaining());
        in.readBytes(readMsg);
        readMsg.flip();
        assertTrue("rcv msg verify", readMsg.compareTo(
                msgRcv.duplicate()) == 0);
        resetKeepAliveTimer();
    }
}
