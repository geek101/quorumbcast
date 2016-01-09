package com.quorum.netty;

import com.quorum.QuorumServer;
import com.quorum.Vote;
import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import com.quorum.util.LogPrefix;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

import java.net.InetSocketAddress;

import static com.quorum.netty.VotingChannel.ReaderState.MSGLEN;
import static com.quorum.util.InitMessageCtx.PROTOCOL_VERSION;
import static com.quorum.util.InitMessageCtx.getAddrString;
import static io.netty.buffer.Unpooled.buffer;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * This is a duplex channel but supports an outgoing connection. Use this
 * for connecting side.
 */
public class VotingClientChannel extends VotingChannel {
    private static final Logger LOGS
            = getLogger(VotingClientChannel.class.getName());

    public VotingClientChannel(
            final long mysid, final InetSocketAddress myElectionAddr,
            final QuorumServer server, final long readMsgTimeoutMsec,
            final long connectMsgTimeoutMsec,
            final long keepAliveTimeoutMsec,
            final int keepAliveCount,
            final Callback<Vote> msgRxCb) {
        super(mysid, myElectionAddr, server, readMsgTimeoutMsec,
                connectMsgTimeoutMsec, keepAliveTimeoutMsec,
                keepAliveCount, msgRxCb);
        readerState = MSGLEN;
        this.LOG = new LogPrefix(LOGS, "my.sid:" + this.mySid + "-" + "sid:"
                + server.id() + "-");
    }

    public VotingClientChannel(
            long mysid, InetSocketAddress myElectionAddr,
            QuorumServer server, long readMsgTimeoutMsec,
            long connectMsgTimeoutMsec,
            Callback msgRxCb) {
        this(mysid, myElectionAddr, server, readMsgTimeoutMsec,
                connectMsgTimeoutMsec, 0, 0, msgRxCb);
    }

    @Override
    protected ByteBuf buildHdr(ChannelHandlerContext ctx) {
        ByteBuf buf = buffer(2046);
        final String electionAddrStr
                = getAddrString(myElectionAddr);
        buf.writeLong(PROTOCOL_VERSION)
                .writeLong(this.mySid)
                .writeInt(electionAddrStr.getBytes().length)
                .writeBytes(electionAddrStr.getBytes());
        return buf;
    }

    /**
     * No hdr rx for connecting channel.
     *
     * @param ctx
     * @param in
     * @return
     */
    @Override
    protected boolean readHdr(ChannelHandlerContext ctx, ByteBuf in) {
        return true;
    }

    @Override
    protected void connected(ChannelHandlerContext ctx)
            throws ChannelException {
        super.connected(ctx);
    }
}
