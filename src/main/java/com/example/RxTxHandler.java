package com.example;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by powell on 11/23/15.
 */
public abstract class RxTxHandler extends ChannelDuplexHandler {
    private static final Logger LOG
            = LoggerFactory.getLogger(RxTxHandler.class);

    /**
     * Use this to perform what to do when connected.
     * @param ctx
     * @throws ChannelException
     */
    protected void connected(ChannelHandlerContext ctx)
            throws ChannelException {
        ByteBuf b = sendHdr();
        if (b != null) {
            ctx.writeAndFlush(b);
        }
    }

    /**
     * Use this to perform what to do when connection is closed.
     * @param ctx
     * @throws ChannelException
     */
    protected void disconnected(ChannelHandlerContext ctx)
            throws ChannelException {}

    /**
     * Connected event forwarder.
     * @param ctx
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        try {
            connected(ctx);
            super.channelActive(ctx);
        } catch (Exception exp) {
            LOG.error("Exception on active: " + exp);
            ctx.close();
        }
    }

    /**
     * Disconnected event forwarder.
     * @param ctx
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        try {
            disconnected(ctx);
            super.channelInactive(ctx);
        } catch (Exception exp) {
            LOG.error("Exception on active: " + exp);
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    /**
     * Overload this to send an hdr.
     * @return
     */
    protected ByteBuf sendHdr() {
        return null;
    }

    protected boolean readHdr() {
        return true;
    }
}
