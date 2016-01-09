package com.quorum.util;

import com.quorum.AbstractServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

/**
 * Created by powell on 12/8/15.
 */
public class AsyncClientSocket extends AsyncSocket<Socket> {
    private static final Logger LOG =
            LoggerFactory.getLogger(AsyncClientSocket.class.getName());

    public AsyncClientSocket(final Socket socket,
                             ExecutorService executorService) {
        super(socket, executorService);
    }

    public AsyncClientSocket(final Socket socket) {
        super(socket);
    }

    public FutureTask<Socket> connect(final AbstractServer server) {
        FutureTask<Socket> futureTask = new FutureTask<Socket>(
                new Callable<Socket>() {
                    @Override
                    public Socket call() throws IOException {
                        connectSync(server);
                        return getSocket();
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    public FutureTask<Socket> connect(final AbstractServer server,
                                      final int timeout) {
        FutureTask<Socket> futureTask = new FutureTask<>(
                new Callable<Socket>() {
                    @Override
                    public Socket call() throws IOException {
                        connectSync(server, timeout);
                        return getSocket();
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    public FutureTask<String> read() {
        FutureTask<String> futureTask = new FutureTask<>(
                new Callable<String>() {
                    @Override
                    public String call() throws IOException {
                        return readSync();
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    public FutureTask<Void> write(final String msg) {
        FutureTask<Void> futureTask = new FutureTask<>(
                new Callable<Void>() {
                    @Override
                    public Void call() throws IOException {
                        return writeSync(msg);
                    }
                });
        submitTask(futureTask);
        return futureTask;
    }

    protected String readSync() throws IOException {
        String str;
        final Socket s = getSocket();
        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(s.getInputStream()))) {
            str = br.readLine();
        } catch (IOException exp) {
            LOG.error("read error, exp: " + exp);
            throw exp;
        }

        return str;
    }

    protected Void writeSync(final String msg) throws IOException {
        PrintWriter pw = new PrintWriter(getSocket().getOutputStream(), true);
        pw.println(msg);
        pw.flush();
        pw.close();
        return null;
    }

    private void connectSync(final AbstractServer server)
            throws IOException {
        setOptions();
        getSocket().connect(server.getElectionAddr(), server.getElectionAddr().getPort());
    }

    private void connectSync(final AbstractServer server, final int timeout)
            throws IOException {
        setOptions();
        getSocket().connect(server.getElectionAddr(), timeout);
    }

    private void setOptions() throws SocketException {
        getSocket().setSoLinger(true, 0);
        getSocket().setTcpNoDelay(true);
        getSocket().setKeepAlive(true);
    }
}
