package com.microsoft.azure.eventhubs.lib;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ProxyNegotiationHandler {

    public enum ProxyConnectionState {
        PROXY_NOT_STARTED,
        PROXY_INITIATED,
        PROXY_CONNECTED,
        PROXY_FAILED,
        PROXY_CLOSED
    }

    private final AsynchronousSocketChannel clientSocket;
    private final AsynchronousSocketChannel serviceSocket;

    private ProxyConnectionState proxyConnectionState;

    public ProxyNegotiationHandler(final AsynchronousSocketChannel clientSocket) throws IOException {

        this.clientSocket = clientSocket;
        this.proxyConnectionState = ProxyConnectionState.PROXY_NOT_STARTED;
        this.serviceSocket = AsynchronousSocketChannel.open();

        ReadWriteState newRead = new ReadWriteState();
        newRead.clientSocket = this.clientSocket;
        newRead.buffer = ByteBuffer.allocate(64 * 1024);
        newRead.isRead = true;
        newRead.isClientWriter = true;
        newRead.serviceSocket = this.serviceSocket;

        this.clientSocket.read(newRead.buffer, newRead, new ReadWriteHandler());
    }

    static class ReadWriteState {
        AsynchronousSocketChannel clientSocket;
        AsynchronousSocketChannel serviceSocket;
        ByteBuffer buffer;
        Boolean isRead;
        Boolean isClientWriter;
    }

    class ReadWriteHandler implements CompletionHandler<Integer, ReadWriteState> {

        @Override
        public void completed(Integer result, ReadWriteState readWriteState) {
            if (result == -1) {
                proxyConnectionState = ProxyConnectionState.PROXY_CLOSED;
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try {
                    serviceSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }

            switch (proxyConnectionState) {
                case PROXY_NOT_STARTED:
                    if (readWriteState.isRead) {
                        ByteBuffer readBuffer = readWriteState.buffer;
                        readBuffer.flip();
                        int bytesToRead = readBuffer.limit();
                        if (bytesToRead > 0) {
                            final byte[] connectRequest = new byte[bytesToRead];
                            readBuffer.get(connectRequest, 0, bytesToRead);
                            readBuffer.compact();
                            final String request = new String(connectRequest, StandardCharsets.UTF_8);
                            final Scanner requestScanner = new Scanner(request);
                            final String firstLine = requestScanner.nextLine();

                            final String hostNamePortParts[] = firstLine
                                    .replace("CONNECT ", "")
                                    .replace(" HTTP/1.1", "")
                                    .split(":");

                            final String hostName = hostNamePortParts[0];
                            final int port = Integer.parseInt(hostNamePortParts[1]);
                            proxyConnectionState = ProxyConnectionState.PROXY_INITIATED;
                            serviceSocket.connect(
                                    new InetSocketAddress(hostName, port),
                                    readWriteState,
                                    new CompletionHandler<Void, ReadWriteState>() {
                                        @Override
                                        public void completed(Void result, ReadWriteState attachment) {
                                            proxyConnectionState = ProxyConnectionState.PROXY_CONNECTED;
                                            attachment.isClientWriter = true;
                                            attachment.isRead = false;
                                            attachment.buffer.clear();
                                            attachment.buffer.put("HTTP/1.1 200 Connection Established\r\n\r\n".getBytes());
                                            attachment.buffer.limit(attachment.buffer.position());
                                            attachment.buffer.flip();
                                            clientSocket.write(attachment.buffer, attachment, ReadWriteHandler.this);

                                            final ReadWriteState serviceReadWriteState = new ReadWriteState();
                                            serviceReadWriteState.isClientWriter = false;
                                            serviceReadWriteState.isRead = true;
                                            serviceReadWriteState.buffer = ByteBuffer.allocate(64 * 1024);
                                            serviceReadWriteState.clientSocket = attachment.clientSocket;
                                            serviceReadWriteState.serviceSocket = attachment.serviceSocket;
                                            clientSocket.read(serviceReadWriteState.buffer, serviceReadWriteState, ReadWriteHandler.this);
                                        }

                                        @Override
                                        public void failed(Throwable exc, ReadWriteState attachment) {
                                            proxyConnectionState = ProxyConnectionState.PROXY_FAILED;

                                            attachment.isClientWriter = true;
                                            attachment.isRead = false;
                                            attachment.buffer.clear();
                                            attachment.buffer.put(String.format("HTTP/1.1 502 %s\r\n\r\n", exc.getMessage()).getBytes());
                                            attachment.buffer.flip();
                                            clientSocket.write(attachment.buffer, attachment, ReadWriteHandler.this);
                                        }
                                    });
                        }
                    } else {
                        throw new IllegalStateException();
                    }
                    break;

                case PROXY_CONNECTED:
                    if (!readWriteState.isRead) {
                        // if this is write_success_callback - issue a read on the same channel
                        final AsynchronousSocketChannel readChannel = !readWriteState.isClientWriter
                                ? readWriteState.clientSocket
                                : readWriteState.serviceSocket;

                        readWriteState.isRead = true;
                        readWriteState.buffer.flip();
                        readWriteState.buffer.clear();
                        showStats(result + " : proxy <- " + (readWriteState.isClientWriter ? "client" : "service"), readWriteState.buffer);
                        readChannel.read(readWriteState.buffer, readWriteState, this);
                    } else {
                        final AsynchronousSocketChannel writeChannel = !readWriteState.isClientWriter
                                ? readWriteState.serviceSocket
                                : readWriteState.clientSocket;

                        readWriteState.isRead = false;
                        readWriteState.buffer.flip();
                        showStats(result + ": proxy -> " + (readWriteState.isClientWriter ? "client" : "service"), readWriteState.buffer);
                        writeChannel.write(readWriteState.buffer, readWriteState, this);
                    }
                    break;

                default:
                    break;
            }

        }

        @Override
        public void failed(Throwable exc, ReadWriteState attachment) {
            exc.printStackTrace();
        }
    }

    private static void showStats( String where, Buffer b )
    {
        System.out.println( where +
                " bufferPosition: " +
                b.position() +
                " limit: " +
                b.limit() +
                " remaining: " +
                b.remaining() +
                " capacity: " +
                b.capacity() );
    }
}
