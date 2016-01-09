package com.example;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by powell on 11/19/15.
 */
public class TxHandler extends RxTxHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TxHandler.class);
    private final Long sid;
    public TxHandler(long sid) {
        this.sid = sid;
    }

    /**
     * Use this to perform what to do when connected.
     * @param ctx
     * @throws ChannelException
     */
    protected void connected(ChannelHandlerContext ctx) throws
            ChannelException {
        LOG.info("connected: " + ctx.channel().localAddress()
                + " - " + ctx.channel().remoteAddress());
        super.connected(ctx);
    }

    /**
     * Use this to perform what to do when connection is closed.
     * @param ctx
     * @throws ChannelException
     */
    protected void disconnected(ChannelHandlerContext ctx) throws
            ChannelException {
        LOG.info("disconnected: " + ctx.channel().localAddress()
                + " - " + ctx.channel().remoteAddress());
    }

    public void write(ChannelHandlerContext ctx,
                      Object msg,
                      ChannelPromise promise)
            throws Exception {
        ByteBuf b = Unpooled.buffer(Message.SIZE);
        b.writeLong(this.sid);
        b.writeInt((Integer)msg);

        ctx.writeAndFlush(b);
    }

    /**
     * Helper to send header on connect.
     * @return the hdr to send
     */
    @Override
    protected ByteBuf sendHdr() {
        ByteBuf b = Unpooled.buffer(Long.BYTES);
        b.writeLong(Message.version);
        return b;
    }
}
