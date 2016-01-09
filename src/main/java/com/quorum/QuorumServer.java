/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>http://www.apache.org/licenses/LICENSE-2.0</p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.quorum;

import com.quorum.util.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.PatternSyntaxException;

/**
 * Class for a single server which is part of the Quorum.
 */
public class QuorumServer implements AbstractServer {
    private static final Logger LOG =
            LoggerFactory.getLogger(QuorumServer.class);
    private long id;                        /// A unique id for this server.
    private String hostname;
    public QuorumPeer.LearnerType learnerType
            = QuorumPeer.LearnerType.PARTICIPANT;
    private InetSocketAddress addr = null;  /// IP and Port for this server.
    private InetSocketAddress electionAddr = null;

    private QuorumServer(final long id, final QuorumPeer.LearnerType
            learnerType) {
        this.id = id;
        this.learnerType = learnerType;
    }

    private QuorumServer(final long id) {
        this(id, QuorumPeer.LearnerType.PARTICIPANT);
    }

    /**
     * Set id and address of the server.
     * @param id
     * @param addr
     */
    public QuorumServer(long id, InetSocketAddress addr) {
        this(id);
        this.electionAddr = addr;
    }

    public QuorumServer(long id, InetSocketAddress addr,
                         InetSocketAddress electionAddr) {
        this(id, addr);
        this.electionAddr = electionAddr;
    }

    public QuorumServer(long id, InetSocketAddress addr,
                         InetSocketAddress electionAddr,
                         QuorumPeer.LearnerType learnerType) {
        this(id, addr, electionAddr);
        this.learnerType = learnerType;
    }

    public QuorumServer(long id, String hostname,
                        Integer port, Integer electionPort,
                        QuorumPeer.LearnerType learnerType)
            throws ChannelException {
        this(id, learnerType);
        this.hostname = hostname;
        this.addr = getSocketAddr(resolveHostname(hostname), port);
        this.electionAddr = getSocketAddr(resolveHostname(hostname),
                electionPort);
    }

    /**
     * addressStr example: "localhost:2888:3888"
     * works for "localhost:2888" too.
     *
     * @param id
     * @param addressStr
     * @throws ChannelException
     */
    public QuorumServer(long id, String addressStr) throws ChannelException {
        this(id);

        // Parse for hostname and two ports
        String[] parts =  null;
        try {
            parts = addressStr.split(":", 3);
        } catch (PatternSyntaxException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("addressStr[" )
                    .append(addressStr)
                    .append("] parse err: ")
                    .append(e);
            LOG.warn(sb.toString());
            throw new ChannelException(sb.toString());
        }

        this.hostname = parts[0];

        // Resolve for zab port
        this.addr = getSocketAddr(resolveHostname(
                this.hostname), parsePort(parts[1]));

        if (parts.length > 2) {
            this.electionAddr = getSocketAddr(resolveHostname(
                    this.hostname), parsePort(parts[2]));
        }
    }

    private InetAddress resolveHostname(final String hostname)
            throws ChannelException {
        try {
            return InetAddress.getByName(hostname);
        } catch (UnknownHostException ex) {
            StringBuilder sb = new StringBuilder();
            sb.append("Failed to resolve address:")
                    .append(hostname)
                    .append(" exp:")
                    .append(ex);
            LOG.warn(sb.toString());
            throw new ChannelException(sb.toString());
        }
    }

    private int parsePort(final String portStr) throws ChannelException {
        int port = -1;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException ex) {
            StringBuilder sb = new StringBuilder();
            sb.append("Failed to parse port:")
                    .append(portStr)
                    .append(" exp:")
                    .append(ex);
            LOG.warn(sb.toString());
            throw new ChannelException(sb.toString());
        }

        if (port > (Short.MAX_VALUE*2 - 1)) {
            StringBuilder sb = new StringBuilder();
            sb.append("Invalid parsed port: ")
                    .append(port);
            LOG.warn(sb.toString());
            throw new ChannelException(sb.toString());
        }

        return port;
    }

    private InetSocketAddress getSocketAddr(final InetAddress address,
                                            final int port)
            throws ChannelException {
        try {
            return new InetSocketAddress(address, port);
        } catch (IllegalArgumentException ex) {
            StringBuilder sb = new StringBuilder();
            sb.append("Error InetSocketAddress exp: ")
                    .append(ex);
            LOG.warn(sb.toString());
            throw new ChannelException(sb.toString());
        }
    }

    /**
     * Performs a DNS lookup of hostname and (re)creates the this.addr and
     * this.electionAddr InetSocketAddress objects as appropriate
     *
     * If the DNS lookup fails, this.addr and electionAddr remain
     * unmodified, unless they were never set. If this.addr is null, then
     * it is set with an unresolved InetSocketAddress object. this.electionAddr
     * is handled similarly.
     */
    public void recreateSocketAddresses() {
        InetAddress address = null;
        try {
            address = InetAddress.getByName(this.hostname);
            LOG.info("Resolved hostname: {} to address: {}", this.hostname,
                    address);
            this.addr = new InetSocketAddress(address, this.addr.getPort());
            if (this.electionAddr.getPort() > 0){
                this.electionAddr = new InetSocketAddress(address,
                        this.electionAddr.getPort());
            }
        } catch (UnknownHostException ex) {
            LOG.warn("Failed to resolve address: {}", this.hostname, ex);
            // The hostname has never resolved. Create our
            // InetSocketAddress(es) as unresolved
            this.addr = InetSocketAddress.createUnresolved(this.hostname,
                    this.addr.getPort());
            if (this.electionAddr.getPort() > 0){
                this.electionAddr = InetSocketAddress.createUnresolved(
                        this.hostname, this.electionAddr.getPort());
            }
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("server.")
                .append(id)
                .append("=");
        if (addr != null) {
            sb.append(addr.toString());
        }
        if (electionAddr != null) {
            sb.append(electionAddr.toString());
        }
        return sb.toString();
    }

    public InetSocketAddress getAddr() { return addr; }

    public InetSocketAddress getElectionAddr() { return electionAddr; }

    public long id() {
        return id;
    }

    @Override
    public int hashCode() { return (int)id; }

    @Override
    public boolean equals(Object other) {
        if (other instanceof QuorumServer &&
                id == ((QuorumServer)other).id()) {
            return true;
        }
        return false;
    }

    public QuorumPeer.LearnerType getLearnerType() {
        return learnerType;
    }

    public void setLearnerType(final QuorumPeer.LearnerType learnerType) {
        this.learnerType = learnerType;
    }
}
