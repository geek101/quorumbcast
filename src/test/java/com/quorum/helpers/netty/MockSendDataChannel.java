package com.quorum.helpers.netty;

import com.quorum.util.ChannelException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;

/**
 * Created by powell on 12/6/15.
 */
public class MockSendDataChannel extends MockSndHdrChannel {
    public MockSendDataChannel(final long key, final ByteBuffer hdrData) {
        super(key, hdrData, 0);
    }

    public MockSendDataChannel(final long key, final ByteBuffer hdrData,
                             long connectTimeoutMsec) {
        super(key, hdrData, connectTimeoutMsec);
    }

    public MockSendDataChannel(final long key, final ByteBuffer hdrData,
                               long connectTimeoutMsec,
                               final long keepAliveTimeoutMsec,
                               final int keepAliveCount) {
        super(key, hdrData, connectTimeoutMsec, keepAliveTimeoutMsec,
                keepAliveCount);
    }

    @Override
    protected ByteBuf buildHdr(ChannelHandlerContext ctx) {
        return getHdr();
    }
}
