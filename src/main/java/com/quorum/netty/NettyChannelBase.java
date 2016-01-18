package com.quorum.netty;

import com.quorum.util.ChannelException;
import com.quorum.util.LogPrefix;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.handler.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;

public abstract class NettyChannelBase extends ByteToMessageDecoderImpl {
    protected static final Logger LOGS
            = LoggerFactory.getLogger(NettyChannel.class.getName());
    private Channel channel = null;
    protected String prefixStr = null;
    protected LogPrefix LOG = new LogPrefix(LOGS, "");

    public void setChannel(Channel channel) throws ChannelException {
        if (this.channel != null) {
            throw new ChannelException("Invalid channel is already set to:"
                    + this.channel);
        }
        this.channel = channel;
        prefixStr = getPrefix(channel);
    }

    public final ChannelId getChannelId()
            throws ChannelException {
        if (this.channel == null) {
            throw new ChannelException(
                    "Invalid call to getChannelId, when channel is not set");
        }
        return this.channel.id();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        try {
            connected(ctx);
            super.channelActive(ctx);
        } catch (Exception exp) {
            LOG.error("Exception on active: " + exp);
            errClose(ctx);
        }

        prefixStr = getPrefix(ctx.channel());
        LOG.appendPrefix(prefixStr);
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
            errClose(ctx);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("exception cause: " + cause);
        if (cause instanceof IOException ||
                cause instanceof ChannelException) {
            errClose(ctx);
        } else if (cause instanceof DecoderException) {
            if (cause.getCause() != null) {
                Throwable th = cause.getCause();
                if (th instanceof SSLException) {
                    errClose(ctx);
                } else {
                    th.printStackTrace();
                    LOG.error("Unhandled error, shutting down: ", th);
                    System.exit(-1);
                }
            } else {
                LOG.error("decoder err: " + cause);
                System.exit(-1);
            }
        } else {
            cause.printStackTrace();
            LOG.error("Unhandled error, shutting down: ", cause);
            System.exit(-1);
        }
    }

    /**
     * Use this to perform what to do when connected.
     * @param ctx
     * @throws ChannelException
     */
    protected abstract void connected(ChannelHandlerContext ctx)
            throws ChannelException;

    /**
     * Use this to perform what to do when connection is closed.
     * @param ctx
     * @throws ChannelException
     */
    protected abstract void disconnected(ChannelHandlerContext ctx)
            throws ChannelException;


    protected abstract void errClose(ChannelHandlerContext ctx);

    protected final String getChannelStr() {
        return prefixStr;
    }

    /**
     * Override this for adding application specific code.
     * @param channel
     * @return String representing this channel.
     */
    protected static final String getPrefix(final Channel channel) {
        StringBuilder sb = new StringBuilder();
        sb.append(channel.id().asShortText())
                .append('-')
                .append(channel.localAddress())
                .append("--")
                .append(channel.remoteAddress());
        return sb.toString();
    }

    protected final Channel getChannel() {
        return this.channel;
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
    }

    protected void sendMsg(Object msg) {
        if (this.channel == null) {
            final String errStr
                    = "Invalid channel is not set, cannot send msg";
            LOG.error(errStr);
            throw new RuntimeException(errStr);
        }
        this.channel.writeAndFlush(msg);
    }
}
