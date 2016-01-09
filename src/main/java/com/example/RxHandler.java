package com.example;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by powell on 11/19/15.
 */
public class RxHandler extends RxTxHandler {
    private static final Logger LOG
            = LoggerFactory.getLogger(RxHandler.class);
    public final Callback cb;
    public RxHandler(Callback cb) {
        this.cb = cb;
    }

    private ByteBuf buf;
    private Long version = null;

    /**
     * Use this to perform what to do when connected.
     * @param ctx
     * @throws ChannelException
     */
    protected void connected(ChannelHandlerContext ctx) throws
            ChannelException {
        LOG.info("connected: " + ctx.channel().localAddress()
                + " - " + ctx.channel().remoteAddress());
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

    /**
     * Handler added to ctx i.e handing data?.
     * @param ctx
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        buf = ctx.alloc().buffer(Message.SIZE);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        buf.release();
        buf = null;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf m = (ByteBuf) msg;
        buf.writeBytes(m);
        m.release();

        // read hdr first
        if (version == null &&
                buf.readableBytes() >= Long.BYTES &&
                (version = buf.readLong()) != Message.version) {
            LOG.error("Invalid version received: " + version);
            ctx.close();
            return;
        }

        if (version != null &&
                buf.readableBytes() >= Message.SIZE) {
            long sid = buf.readLong();
            int value = buf.readInt();
            cb.done(new Message(sid, value));
        }
    }
}

