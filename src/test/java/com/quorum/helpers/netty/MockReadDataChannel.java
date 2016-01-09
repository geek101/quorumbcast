package com.quorum.helpers.netty;

import java.nio.ByteBuffer;

/**
 * Created by powell on 12/6/15.
 */
public class MockReadDataChannel extends MockReadHdrChannel {
    public ByteBuffer readMsg = null;
    public int readMsgCount;
    public MockReadDataChannel(final long key, final int hdrLen) {
        super(key, hdrLen, 0);
    }

    public MockReadDataChannel(final long key, final int hdrLen,
                               final long readTimeoutMsec) {
        super(key, hdrLen, readTimeoutMsec);
    }

    public void resetReadBuffer() {
        synchronized (this) {
            readMsg = null;
            readMsgCount = 0;
        }
    }
}
