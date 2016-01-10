/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quorum.netty;

import com.common.X509Exception;
import com.quorum.QuorumServer;
import com.quorum.helpers.PortAssignment;
import com.quorum.util.AsyncClientSocket;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test various aspects of SSL sockets.
 * Check that SocketFactory works for server side and client side
 * and also verify that using an invalid CA will ensure connection will
 * terminate.
 * Created by powell on 12/8/15.
 */
public class QuorumSocketFactoryTest extends BaseTest {
    private static final Logger LOG
            = LoggerFactory.getLogger(QuorumSocketFactoryTest.class);

    private final QuorumServer listenServer
            = new QuorumServer(1, new InetSocketAddress("localhost",
            PortAssignment.unique()));
    private final QuorumServer client1
            = new QuorumServer(2, new InetSocketAddress("localhost",
            PortAssignment.unique()));
    private final QuorumServer client2
            = new QuorumServer(3, new InetSocketAddress("localhost",
            PortAssignment.unique()));

    @Before
    public void setup() {
        this.sslEnabled = true;
    }

    @After
    public void cleanUp() {
        if (clientSocket1 != null) {
            close(clientSocket1);
        }

        if (clientSocket2 != null) {
            close(clientSocket2);
        }

        if (serverSocket != null) {
            close(serverSocket);
        }
    }

    @Test
    public void testListener() throws Exception {
        serverSocket = newServerAndBindTest(listenServer, 0);
        serverSocket.close();
        serverSocket = null;
    }

    @Test
    public void testAccept() throws Exception {
        final int serverIndex = 0;
        final int clientIndex = 1;

        Collection<Socket> sockets = connectOneClientToServerTest(
                client1, listenServer, clientIndex, serverIndex);
        Iterator<Socket> it = sockets.iterator();
        it.next();
        it.next().close();
        clientSocket1.close();
        clientSocket1 = null;
        serverSocket.close();
        serverSocket = null;
    }

    @Test(expected=SSLHandshakeException.class)
    public void testBadClient() throws X509Exception, InterruptedException,
            ExecutionException, IOException, NoSuchAlgorithmException {
        readWriteTestHelper(connectOneBadClientToServerTest(
                client1, listenServer, 0, 1), "HelloWorld!");
    }

    @Test
    public void testReadWrite() throws X509Exception, InterruptedException,
            ExecutionException, IOException, NoSuchAlgorithmException {
        readWriteTestHelper(connectOneClientToServerTest(
                client1, listenServer, 0, 1), "HelloWorld!");
    }

    private void readWriteTestHelper(Collection<Socket> sockets,
                                     final String testStr)
            throws InterruptedException, ExecutionException, IOException {
        Iterator<Socket> it = sockets.iterator();
        // Write from client
        FutureTask<Void> writeFuture
                = new AsyncClientSocket(it.next()).write(testStr);
        FutureTask<String> readFuture
                = new AsyncClientSocket(it.next()).read();

        while(!writeFuture.isDone()
                || !readFuture.isDone()) {
            Thread.sleep(2);
        }

        String str = null;
        try {
            str = readFuture.get();
        } catch (ExecutionException exp) {
            if (exp.getCause() != null &&
                    exp.getCause() instanceof IOException) {
                if (exp.getCause() instanceof SSLHandshakeException) {
                    throw (SSLHandshakeException) exp.getCause();
                } else {
                    throw (IOException) exp.getCause();
                }
            } else {
                throw exp;
            }
        }

        assertEquals("data txrx", testStr, str);
        it = sockets.iterator();
        it.next();
        it.next().close();
        clientSocket1.close();
        clientSocket1 = null;
        serverSocket.close();
        serverSocket = null;
    }
}

