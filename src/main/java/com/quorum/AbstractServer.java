package com.quorum;

import java.net.InetSocketAddress;

/**
 * Created by powell on 11/25/15.
 */
public interface AbstractServer {
    public InetSocketAddress getElectionAddr();
}
