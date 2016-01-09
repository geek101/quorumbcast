package com.quorum.helpers.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;

/**
 * Helper class to read hdr only.
 * Use getHdrReceived() can return null when hdr has still not being received.
 * Created by powell on 12/6/15.
 */
public class MockReadHdrChannel extends MockChannel {
    private final Long key;
    private final int hdrLen;

    private ByteBuffer hdrData = null;
    public MockReadHdrChannel(final long key, final int hdrLen) {
        this(key, hdrLen, 0);
    }

    public MockReadHdrChannel(final long key, final int hdrLen,
                              final long readTimeoutMsec) {
        super(readTimeoutMsec, 0);
        this.key = key;
        this.hdrLen = hdrLen;
    }

    /**
     * Application level unique key for this channel.
     * @return
     */
    public Long key() {
        return key;
    }

    public ByteBuffer getHdrReceived() {
        return hdrData;
    }

    @Override
    protected ByteBuf buildHdr(ChannelHandlerContext ctx) {
        return null;
    }

    @Override
    protected boolean readHdr(ChannelHandlerContext ctx, ByteBuf in) {
        if (in.readableBytes() < hdrLen) {
            return false;
        }

        hdrData = java.nio.ByteBuffer.allocate(hdrLen);
        in.readBytes(hdrData);
        hdrData.flip();
        return true;
    }
}
