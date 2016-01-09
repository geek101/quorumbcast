package com.quorum.netty;

import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.util.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static com.quorum.netty.VotingChannel.ReaderState.*;
import static com.quorum.util.InitMessageCtx.KEY;
import static java.lang.Integer.parseInt;
import static java.lang.Long.BYTES;
import static java.net.InetAddress.getByName;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Used when accepting a connection used for reading hdr.
 * Created by powell on 11/25/15.
 */
public class VotingServerChannel extends VotingChannel {
    private static final Logger LOGS
            = getLogger(VotingServerChannel.class.getName());
    private int hdrReadLen = BYTES;
    private final Callback2 sidLearnedCb;

    public VotingServerChannel(final long mysid,
                               final InetSocketAddress myElectionAddr,
                               final long readMsgTimeoutMsec,
                               final long keepAliveTimeoutMsec,
                               final int keepAliveCount,
                               final Callback2 sidLearnedCb,
                               final Callback<Vote> msgRxCb) {
        super(mysid, myElectionAddr, null, readMsgTimeoutMsec, 0L,
                keepAliveTimeoutMsec, keepAliveCount, msgRxCb);
        this.sidLearnedCb = sidLearnedCb;
        this.LOG = new LogPrefix(LOGS, "my.sid:" + this.mySid + "-"
                + "sid:Unknown" + "-");
    }

    public VotingServerChannel(long mysid, InetSocketAddress myElectionAddr,
                               long readMsgTimeoutMsec,
                               Callback2 sidLearnedCb,
                               Callback msgRxCb) {
        this(mysid, myElectionAddr, readMsgTimeoutMsec, 0L, 0,
                sidLearnedCb, msgRxCb);
    }

    @Override
    protected ByteBuf buildHdr(ChannelHandlerContext ctx) {
        // we need a header from the other side, hence trigger the timer here.
        resetReadTimer(ctx);
        return null;
    }

    @Override
    protected boolean readHdr(ChannelHandlerContext ctx, ByteBuf in) {
        InitMessageCtx initMessageCtx
                = ctx.channel().attr(KEY).get();

        if (in.readableBytes() < hdrReadLen) {
            return false;
        }

        if (initMessageCtx.protocolVersion == null
                && readerState == UNKNOWN) {
            if (readHdrVer(ctx, in, initMessageCtx)) {
                try {
                    sidLearnedCb.done(initMessageCtx.sid, this);
                } catch (ChannelException exp) {
                    LOG.error("Sid learned callback failed: " + exp);
                    errClose(ctx);
                } finally {
                    stopReadTimer(ctx);
                }
                return true;
            } else {
                return false;
            }
        } else if (initMessageCtx.sid == null
                && readerState == HDR) {
            long sid = in.readLong();
            int remainder = in.readInt();
            initMessageCtx.sid = sid;
            initMessageCtx.remainder = remainder;
            hdrReadLen = remainder;
            return false;
        } else if (readerState == HDR) {
            // ok we have everything.
            try {
                readHostAndPort(in, initMessageCtx);
                setServer(new QuorumServer(
                        initMessageCtx.sid, initMessageCtx.addr));
                // learned sid try to resolve it
                if (!resolveSid(initMessageCtx.sid)) {
                    LOG.info("Sid resolution failed [ from:" + initMessageCtx
                            .sid + " - to:" + this.mySid + " ] going down!");
                    errClose(ctx);
                    return false;
                }
                sidLearnedCb.done(initMessageCtx.sid, this);
                this.LOG.resetPrefix("my.sid:" + this.mySid + "-"
                        + "sid:" + initMessageCtx.sid + "-" + getChannelStr());
                readerState = MSGLEN;
            } catch (ChannelException exp) {
                LOG.error("Error: " + exp);
                errClose(ctx);
            } finally {
                stopReadTimer(ctx);
            }
            return true;
        } else {
            LOG.error("Invalid hdr processing state: " + readerState);
            errClose(ctx);
            return false;
        }
    }

    private boolean readHdrVer(ChannelHandlerContext ctx, ByteBuf in,
                               InitMessageCtx initMessageCtx) {
        long protocolVersion = in.readLong();
        initMessageCtx.protocolVersion = protocolVersion;

        if (protocolVersion > 0) {
            if (resolveSid(protocolVersion)) {
                // if survived then we learned the sid, us it.
                initMessageCtx.sid = protocolVersion;
                initMessageCtx.addr
                        = (InetSocketAddress) ctx.channel().remoteAddress();
                return true;
            } else {
                errClose(ctx);
                return false;
            }
        } else {
            hdrReadLen = BYTES + Integer.BYTES;
            readerState = HDR;
            return false;
        }
    }

    private boolean resolveSid(Long sid) {
        LOG.debug("Resolving sid: " + sid);
        return sid > this.mySid;
    }

    private void readHostAndPort(ByteBuf in, InitMessageCtx initMsgCtx)
            throws ChannelException {
        byte[] b = new byte[initMsgCtx.remainder];
        in.readBytes(b);

        // FIXME: IPv6 is not supported. Using something like Guava's
        // HostAndPort parser would be good.
        String addr = new String(b);
        String[] host_port = addr.split(":");

        if (host_port.length != 2) {
            throw new ChannelException(
                    "Badly formed address: %s", addr);
        }

        int port;
        try {
            port = parseInt(host_port[1]);
        } catch (NumberFormatException exp) {
            throw new ChannelException("Bad port number: %s",
                    host_port[1] + " exp: " + exp);
        }

        try {
            initMsgCtx.addr = new InetSocketAddress(
                    getByName(host_port[0]), port);
        } catch (UnknownHostException exp) {
            throw new ChannelException("Unknown host: " + host_port[0],
                    " exception: " + exp);
        }
    }
}

