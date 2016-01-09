package com.quorum.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * Created by powell on 12/8/15.
 */
public abstract class AsyncSocket<T extends Closeable> {
    private static final Logger LOG =
            LoggerFactory.getLogger(AsyncSocket.class.getName());

    private final ExecutorService executorService;
    private final T socket;

    public AsyncSocket(final T socket,
                       final ExecutorService executorService) {
        this.socket = socket;
        this.executorService = executorService;
    }

    public AsyncSocket(final T socket) {
        this(socket, Executors.newSingleThreadExecutor());
    }

    public final T getSocket() {
        return socket;
    }

    public void close() {
        executorService.shutdownNow();
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {}
    }

    protected void submitTask(FutureTask task) {
        executorService.submit(task);
    }
}
