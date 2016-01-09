package com.quorum.netty;

import com.quorum.util.Callback;
import com.quorum.util.ChannelException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Provides read support via the ByteToMessageDecoder,
 * Also provides sendHdr() as first write on channel being active and first
 * read as readHdr() if implementation wants to do that.
 * Supports two outstanding timers used for read timeout and connect timeout.
 * Borrows that from ReadTimeoutHandler implementation.
 * Created by powell on 11/24/15.
 */
public abstract class NettyChannel<T> extends NettyChannelBase {
    private final Long connectTimeoutNanos;
    private final Long readTimeoutNanos;
    private final Long keepAliveTimeoutNanos;
    private final Integer keepAliveCount;
    private final NettyChannel self;

    private TimeoutTask connectTimeoutTask;
    private TimeoutTask readTimeoutTask;
    private KeepAliveTask keepAliveTask;

    private boolean readHdrDone = false;  /// helper for read hdr.
    private boolean stopProcessing = false;  /// If set no read/write processing
                                             // happens
    private final class ReadTimeoutCb implements Callback<
                                                     ChannelHandlerContext> {
        @Override
        public void call(final ChannelHandlerContext o)
                throws ChannelException {
            self.readTimeOut(o);
        }
    }

    private final class ConnectTimeoutCb
            implements Callback<ChannelHandlerContext> {
        @Override
        public void call(final ChannelHandlerContext o) throws ChannelException {
            self.connectTimeOut(o);
        }
    }

    private final class KeepAliveTimeoutCb
            implements Callback<ChannelHandlerContext> {
        @Override
        public void call(final ChannelHandlerContext o) throws ChannelException {
            self.keepAliveTimeOut(o);
        }
    }

    public NettyChannel(final long readTimeoutNanos,
                        final long connectTimeoutNanos,
                        final long keepAliveTimeoutNanos,
                        final int keepAliveCount) {
        super();
        if (connectTimeoutNanos > 0) {
            this.connectTimeoutNanos = connectTimeoutNanos;
        } else {
            this.connectTimeoutNanos = null;
        }

        if (readTimeoutNanos > 0) {
            this.readTimeoutNanos = readTimeoutNanos;
        } else {
            this.readTimeoutNanos = null;
        }

        if (keepAliveTimeoutNanos > 0 && keepAliveCount > 0) {
            this.keepAliveTimeoutNanos = keepAliveTimeoutNanos;
            this.keepAliveCount = keepAliveCount;
        } else {
            this.keepAliveTimeoutNanos = null;
            this.keepAliveCount = null;
        }
        this.self = this;
    }

    public NettyChannel(final long readTimeoutNanos,
                        final long connectTimeoutNanos) {
        this(readTimeoutNanos, connectTimeoutNanos, 0, 0);
    }


    public NettyChannel() {
        this(0, 0, 0, 0);
    }

    /**
     * Application level unique key for this channel.
     * @return
     */
    public abstract T key();

    /**
     * Overload this to send an hdr.
     * @return
     */
    protected abstract ByteBuf buildHdr(ChannelHandlerContext ctx);

    protected abstract boolean readHdr(ChannelHandlerContext ctx, ByteBuf in);

    protected abstract void readMsg(ChannelHandlerContext ctx, ByteBuf in)
            throws ChannelException;

    protected void resetConnectTimer() throws ChannelException {
        resetTimer(connectTimeoutTask);
    }

    protected void resetReadTimer() throws ChannelException {
        resetTimer(readTimeoutTask);
    }

    protected void resetKeepAliveTimer() throws ChannelException {
        resetTimer(keepAliveTask);
    }

    protected void stopConnectTimer() throws ChannelException {
        stopTimer(connectTimeoutTask);
    }

    protected void stopReadTimer() throws ChannelException {
        stopTimer(readTimeoutTask);
    }

    protected void stopKeepAliveTimer() throws ChannelException {
        stopTimer(keepAliveTask);
    }

    private void stopTimer(TimeoutTask task) {
        if (task != null) {
            task.stop();
        }
    }

    private void resetTimer(TimeoutTask task) {
        if (task != null) {
            task.reset();
        }
    }

    /**
     * Start the connect timer.if required.
     * @param ctx
     * @param remoteAddress
     * @param localAddress
     * @param promise
     * @throws Exception
     */
    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                SocketAddress localAddress, ChannelPromise promise)
            throws Exception {
        if (connectTimeoutNanos != null) {
            connectTimeoutTask = new TimeoutTask(ctx,
                    connectTimeoutNanos, new ConnectTimeoutCb());
            connectTimeoutTask.start();
        }
        super.connect(ctx, remoteAddress, localAddress, promise);
    }

    /**
     * Start the read timer if required and stops the connect timer if
     * enabled.
     * @param ctx
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (readTimeoutNanos != null) {
            readTimeoutTask = new TimeoutTask(ctx,
                    readTimeoutNanos, new ReadTimeoutCb());
        }



        stopTimer(connectTimeoutTask);

        ByteBuf b = buildHdr(ctx);
        if (b != null) {
            ctx.writeAndFlush(b);
        }

        super.channelActive(ctx);
    }

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        LOG.debug("decoding");
        if (stopProcessing) {
            return;
        } else {
            if (!readHdrDone) {
                // Hdr reading is not done.
                //LOG.debug("decode: readHdr");
                readHdrDone = readHdr(ctx, in);
                if (readHdrDone && !stopProcessing) {
                    //LOG.debug("decode: readmsg0");
                    readMsg(ctx, in);
                }
                return;
            }
        }

        LOG.debug("decode: readmsg");
        readMsg(ctx, in);
    }

    @Override
    protected void sendMsg(Object msg) {
        // On send we ensure keepalive task knows about this write.
        if (keepAliveTask != null) {
            keepAliveTask.setLastWrite();
        }
        super.sendMsg(msg);
    }

    /**
     * Is called when connect timeout has triggered. Override if required
     * but do not forget to call super(). Will close the connection on timeout.
     * @param ctx
     * @throws Exception
     */
    protected void connectTimeOut(ChannelHandlerContext ctx)
            throws ChannelException {
        LOG.error("connect timeout, closing channel");
        stopConnectTimer();
        errClose(ctx);
    }

    /**
     * Is called when a read timeout was detected. Override if required
     * but do not forget to call super().
     */
    protected void readTimeOut(ChannelHandlerContext ctx)
            throws ChannelException {
        LOG.error("read timeout, closing channel");
        stopReadTimer();
        errClose(ctx);
    }

    /**
     * Is called when a read timeout was detected. Override if required
     * but do not forget to call super().
     */
    protected void keepAliveTimeOut(ChannelHandlerContext ctx)
            throws ChannelException {
        LOG.error("keepalive timeout, closing channel");
        stopKeepAliveTimer();
        errClose(ctx);
    }

    protected void startKeepAliveTimer(ChannelHandlerContext ctx)
            throws ChannelException {
        // Create the keepAliveTask it wont be running though.
        if (keepAliveTimeoutNanos != null && keepAliveCount != null
                && keepAliveTask == null) {
            LOG.info("keep alive task created");
            keepAliveTask = new KeepAliveTask(ctx, keepAliveTimeoutNanos,
                    new KeepAliveTimeoutCb(), keepAliveCount);
            resetKeepAliveTimer();
        }
    }

    @Override
    protected void errClose(ChannelHandlerContext ctx) {
        setStopProcessing();
        try {
            stopConnectTimer();
            stopReadTimer();
        } catch (ChannelException exp) {
            LOG.info("error stopping timer tasks ignoring, exp: " + exp);
        }
        ctx.close();
    }

    /**
     * Override for keepalive timeout for send side.
     * @throws ChannelException
     * @throws IOException
     */
    protected abstract void keepAliveSend(final ChannelHandlerContext ctx)
            throws ChannelException, IOException;

    protected void setStopProcessing() {
        stopProcessing = true;
    }

    private class KeepAliveTask extends TimeoutTask {
        private final Long keepAliveTimeoutNanos;  /// Timeout between each
                                                   // keepalive message
        private final Integer keepAliveCount;  /// No of timeout count for
                                                // error.
        private Long lastWriteTimeNanos;

        private int keepAliveMissCount;
        KeepAliveTask(final ChannelHandlerContext ctx,
                      final long keepAliveTimeoutNanos,
                      final Callback<ChannelHandlerContext> timeoutCb,
                      final int keepAliveCount) {
            super(ctx, keepAliveTimeoutNanos, timeoutCb);
            this.keepAliveTimeoutNanos = keepAliveTimeoutNanos;
            this.keepAliveCount = keepAliveCount;
            this.keepAliveMissCount = 0;
        }

        @Override
        public void start() {
            this.keepAliveMissCount = 0;
            setLastWrite();
            super.start();
        }

        public void setLastWrite() {
            lastWriteTimeNanos = System.nanoTime();
        }

        /**
         * Override this for better control.
         * @throws ChannelException
         * @throws IOException
         */
        @Override
        protected void call() throws ChannelException, IOException {
            if (++keepAliveMissCount > keepAliveCount) {
                super.call();
            } else {
                setLastUse();
                // if last write is well before this timeout then call send
                if (System.nanoTime() > lastWriteTimeNanos) {
                    final long nextDelay = keepAliveTimeoutNanos -
                            (System.nanoTime() - lastWriteTimeNanos);
                    if (nextDelay <= 0) {
                        LOG.info("Keepalive write timeout");
                        keepAliveSend(ctx);
                    }
                }
            }
        }
    }

    private class TimeoutTask implements Runnable {
        protected final ChannelHandlerContext ctx;
        private final long timeoutNanos;
        private final Callback<ChannelHandlerContext> timeoutCb;

        private ScheduledFuture<?> timeoutFuture = null;
        private boolean timerActive = false;
        private long lastUseTime;

        TimeoutTask(final ChannelHandlerContext ctx, final long timeoutNanos,
                    final Callback<ChannelHandlerContext> timeoutCb) {
            this.ctx = ctx;
            this.timeoutNanos = timeoutNanos;
            this.timeoutCb = timeoutCb;
        }

        public void start() {
            if (timeoutFuture != null) {
                stop();
            }
            setLastUse();
            timeoutFuture = ctx.executor().schedule(this,
                    timeoutNanos, TimeUnit.NANOSECONDS);
            timerActive = true;
        }

        public void stop() {
            timerActive = false;
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
                timeoutFuture = null;
            }
        }

        public void reset() {
            stop();
            start();
        }

        protected void setLastUse() {
            lastUseTime = System.nanoTime();
        }
        /**
         * Override this for better control. This just calls error callback.
         * @throws ChannelException
         * @throws IOException
         */
        protected void call() throws ChannelException, IOException {
            timeoutCb.call(ctx);
        }

        @Override
        public void run() {
            if (!timerActive) {
                return;
            }

            if (System.nanoTime() < lastUseTime) {
                return;
            }

            long nextDelay = timeoutNanos - (System.nanoTime() - lastUseTime);

            if (nextDelay <= 0) {
                // time-out - set a new timeout and notify the callback.
                timeoutFuture = ctx.executor().schedule(this, timeoutNanos,
                        TimeUnit.NANOSECONDS);
                try {
                    call();
                } catch (ChannelException | IOException exp) {
                    ctx.fireExceptionCaught(exp);
                } catch (Throwable t) {
                    LOG.error("Unhandled error, shutting down: " + t);
                    System.exit(-1);
                }
            } else {
                // Occurred before the timeout - set a new timeout
                // with shorter delay.
                timeoutFuture = ctx.executor().schedule(this, nextDelay,
                        TimeUnit.NANOSECONDS);
            }
        }
    }
}