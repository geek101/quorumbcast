package com.quorum.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

/**
 * Created by powell on 12/8/15.
 */
public class AsyncServerSocket extends AsyncSocket<ServerSocket> {
    private static final Logger LOG =
            LoggerFactory.getLogger(AsyncServerSocket.class.getName());

    public AsyncServerSocket(final ServerSocket socket,
                             ExecutorService executorService) {
        super(socket, executorService);
    }

    public AsyncServerSocket(final ServerSocket socket) {
        super(socket);
    }

    public FutureTask<Socket> accept() {
        FutureTask<Socket> futureTask = new FutureTask<>(
                new Callable<Socket>() {
                    @Override
                    public Socket call() throws IOException {
                        return acceptSync();
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    private Socket acceptSync() throws IOException {
        Socket s = getSocket().accept();
        setOptions(s);
        return s;
    }

    private void setOptions(Socket s) throws SocketException {
        s.setSoLinger(true, 0);
        s.setTcpNoDelay(true);
        s.setKeepAlive(true);
    }
}
