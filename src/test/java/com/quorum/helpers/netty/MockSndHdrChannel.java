package com.quorum.helpers.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

/**
 * Helper for sending hdr only channel.
 * Created by powell on 12/6/15.
 */
public abstract class MockSndHdrChannel extends MockChannel {
    private final Long key;
    private final ByteBuffer hdrData;
    public MockSndHdrChannel(final long key, final ByteBuffer hdrData) {
        this(key, hdrData, 0);
    }

    public MockSndHdrChannel(final long key, final ByteBuffer hdrData,
                             long connectTimeoutMsec) {
        this(key, hdrData, connectTimeoutMsec, 0, 0);
    }

    public MockSndHdrChannel(final long key, final ByteBuffer hdrData,
                             long connectTimeoutMsec,
                             final long keepAliveTimeoutMsec,
                             final int keepAliveCount) {
        super(0, connectTimeoutMsec, keepAliveTimeoutMsec, keepAliveCount);
        this.key = key;
        this.hdrData = hdrData;
    }

    /**
     * Application level unique key for this channel.
     * @return
     */
    public Long key() {
        return key;
    }

    public ByteBuf getHdr() {
        ByteBuf b = Unpooled.buffer();
        return b.writeBytes(hdrData.duplicate());
    }
}
