package com.quorum.helpers.netty;

import com.quorum.netty.NettyChannel;
import com.quorum.util.ChannelException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by powell on 12/6/15.
 */
public abstract class MockChannel extends NettyChannel<Long> {
    public MockChannel(final long connectTimeoutMsec,
                       final long readTimeoutMsec,
                       final long keepAliveTimeoutMsec,
                       final int keepAliveCount) {
        super(TimeUnit.MILLISECONDS.toNanos(connectTimeoutMsec),
                TimeUnit.MILLISECONDS.toNanos(readTimeoutMsec),
                TimeUnit.MILLISECONDS.toNanos(keepAliveTimeoutMsec),
                keepAliveCount);
    }

    public MockChannel(long connectTimeoutMsec,
                       long readTimeoutMsec) {
        this(connectTimeoutMsec, readTimeoutMsec, 0, 0);
    }

    public MockChannel() {
        super();
    }

    @Override
    protected ByteBuf buildHdr(final ChannelHandlerContext ctx) {
        return null;
    }

    @Override
    protected boolean readHdr(final ChannelHandlerContext ctx,
                              final ByteBuf in) {
        return true;
    }

    @Override
    protected void readMsg(final ChannelHandlerContext ctx,
                           final ByteBuf in) throws ChannelException {
        return;
    }

    /**
     * Use this to perform what to do when connected.
     * @param ctx
     * @throws com.quorum.util.ChannelException
     */
    @Override
    protected void connected(final ChannelHandlerContext ctx)
            throws ChannelException {
        LOG.info("connected");
    }

    /**
     * Use this to perform what to do when connection is closed.
     * @param ctx
     * @throws com.quorum.util.ChannelException
     */
    @Override
    protected void disconnected(final ChannelHandlerContext ctx)
            throws ChannelException {
        LOG.info("disconnected");
    }

    @Override
    protected void keepAliveSend(final ChannelHandlerContext ctx)
            throws ChannelException, IOException {}

    public void sendMsg(ByteBuffer msg) throws ChannelException {
        getChannel().writeAndFlush(Unpooled.buffer().writeBytes(msg));
    }
}
