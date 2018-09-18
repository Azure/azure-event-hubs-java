package com.microsoft.azure.eventhubs.lib;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TestProxy {
    private final int port;

    private AtomicBoolean isRunning;
    private Consumer<Throwable> onErrorHandler;

    TestProxy(final int port) {
        this.port = port;
        this.isRunning = new AtomicBoolean(false);
    }

    public void Start(final Consumer<Throwable> onError) throws IOException {
        if (this.isRunning.get()) {
            throw new IllegalStateException("ProxyServer is already running");
        }

        this.isRunning = new AtomicBoolean(true);
        this.onErrorHandler = onError;

        final AsynchronousServerSocketChannel serverSocket = AsynchronousServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress("localhost", port));
        scheduleListener(serverSocket);
    }

    public Boolean isRunning() {
        return this.isRunning != null && this.isRunning.get();
    }

    private void scheduleListener(final AsynchronousServerSocketChannel serverSocket) {
        serverSocket.accept(
                serverSocket,
                new CompletionHandler<AsynchronousSocketChannel, AsynchronousServerSocketChannel>() {
                    @Override
                    public void completed(
                            AsynchronousSocketChannel client,
                            AsynchronousServerSocketChannel serverSocket) {
                        try {
                            System.out.println("socketacceptedon: " + client.getRemoteAddress().toString());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        serverSocket.accept(serverSocket, this);
                        try {
                            new ProxyNegotiationHandler(client);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void failed(Throwable exc, AsynchronousServerSocketChannel attachment) {
                        isRunning.set(false);
                        onErrorHandler.accept(exc);
                    }
                });
    }
}
