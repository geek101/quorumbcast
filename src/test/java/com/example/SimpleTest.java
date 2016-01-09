package com.example;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

/**
 * Created by powell on 11/19/15.
 */
public class SimpleTest {
    protected static final Logger LOG = LoggerFactory.getLogger(
            SimpleTest.class.getName());
    @Test
    public void testOneConnection() throws InterruptedException {
        LOG.error("error level");
        LOG.warn("warn level");
        LOG.info("info level");
        LOG.debug("debug level");
        Executor executor = Executors.newSingleThreadExecutor();
        EventLoopGroup eventLoopGroup =
                new NioEventLoopGroup(1, executor);

        int t = Long.BYTES;
        InetSocketAddress addr
                = new InetSocketAddress("localhost", 28888);
        Server server = new Server(addr, eventLoopGroup);
        Client client = new Client(1L, addr, eventLoopGroup);

        while(!client.send(123)) {
            Thread.sleep(100);
        }

        Collection<Message> msgs = null;
        while(msgs == null || msgs.size() == 0) {
            msgs = server.getMessages();
        }

        final Message m = msgs.iterator().next();

        assertTrue(m.sid == 1L);
        assertTrue(m.value == 123);

        client.send(9999);
        msgs = null;
        while(msgs == null || msgs.size() == 0) {
            msgs = server.getMessages();
        }

        client.shutdown();
        server.shutdown();
    }
}
