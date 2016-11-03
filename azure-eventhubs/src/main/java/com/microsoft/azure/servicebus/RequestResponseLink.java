/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnknownDescribedType;
import org.apache.qpid.proton.message.Message;

public final class RequestResponseLink extends ClientEntity
{
    static final int RECEIVER_PREFETCH = 50;

    final ISessionProvider sessionProvider;
    final String requestResponsePath;
    final CompletableFuture<RequestResponseLink> openFuture;
    final ConcurrentHashMap<Object, CompletableFuture<Message>> inflightRequests;
    final Duration operationTimeout;
    final ReceiveComplete onReceiveComplete;

    MessageSender sender;
    MessageReceiver receiver;
    AtomicLong requestId;
    
    private RequestResponseLink(
            final MessagingFactory messagingFactory,
            final String clientId,
            final String path,
            final ISessionProvider sessionProvider)
    {
        super(clientId, messagingFactory);
        
        this.sessionProvider = sessionProvider;
        this.requestResponsePath = path;
        this.openFuture = new CompletableFuture<>();
        this.requestId = new AtomicLong(0);
        this.inflightRequests = new ConcurrentHashMap<>();
        this.operationTimeout = messagingFactory.getOperationTimeout();
        this.onReceiveComplete = new ReceiveComplete();
    }
    
    public static CompletableFuture<RequestResponseLink> create(
        final MessagingFactory factory,
        final String name,
        final String path)
    {
        RequestResponseLink requestResponseLink = new RequestResponseLink(factory, name, path, factory);
        
        final String sessionId = StringUtil.getRandomString();
        return MessageSender.create(factory, name + ":sender", path, sessionId)
            .thenComposeAsync(new Function<MessageSender, CompletionStage<MessageReceiver>>()
                {
                    @Override
                    public CompletionStage<MessageReceiver> apply(MessageSender sender)
                    {
                        requestResponseLink.sender = sender;
                        return MessageReceiver.create(
                                factory, 
                                name + ":receiver", 
                                path, 
                                RECEIVER_PREFETCH, 
                                new IReceiverSettingsProvider()
                                {
                                    @Override
                                    public Map<Symbol, UnknownDescribedType> getFilter(Message lastReceivedMessage)
                                    {
                                        return null;
                                    }

                                    @Override
                                    public Map<Symbol, Object> getProperties()
                                    {
                                        return null;
                                    }
                                },
                                sessionId);
                    }
                })
            .thenComposeAsync(new Function<MessageReceiver, CompletionStage<RequestResponseLink>>()
                {
                    @Override
                    public CompletionStage<RequestResponseLink> apply(MessageReceiver receiver)
                    {
                        requestResponseLink.receiver = receiver;
                        requestResponseLink.openFuture.complete(requestResponseLink);
                        return requestResponseLink.openFuture;
                    }
                });
    }
    
    public CompletableFuture<Message> request(final Message message)
    {
        if (message == null)
            throw new IllegalArgumentException("message cannot be null");

        if (message.getMessageId() != null)
            throw new IllegalArgumentException("message.getMessageId() should be null");

        message.setMessageId(this.requestId.incrementAndGet());
        
        CompletableFuture<Message> request = new CompletableFuture<>();
        
        this.inflightRequests.put(message.getMessageId(), request);
        
        Timer.schedule(new RequestTimeout(message.getMessageId()), this.operationTimeout, TimerType.OneTimeRun);

        return this.sender.send(message)
            .thenComposeAsync(new Function<Void, CompletableFuture<Message>>()
            {
                @Override
                public CompletableFuture<Message> apply(Void t)
                {
                    receiver.receive(RECEIVER_PREFETCH).whenCompleteAsync(onReceiveComplete);
                    return request;
                }
            });
    }

    @Override
    protected CompletableFuture<Void> onClose()
    {
        return CompletableFuture.allOf(this.sender.close(), this.receiver.close());
    }
    
    private final class ReceiveComplete implements BiConsumer<Collection<Message>, Throwable>
    {
        @Override
        public void accept(Collection<Message> messages, Throwable error)
        {
            if (messages != null)
            {
                for (Message message: messages)
                {
                    CompletableFuture<Message> inflightRequest = inflightRequests.remove(message.getMessageId());

                    if (inflightRequest != null)
                        inflightRequest.complete(message);
                }
            }
            
            if (error != null)
            {
                Iterator<Map.Entry<Object, CompletableFuture<Message>>> requestsIterator = inflightRequests.entrySet().iterator();
                while (requestsIterator.hasNext())
                {
                    Map.Entry<Object, CompletableFuture<Message>> request = requestsIterator.next();
                    if (request != null)
                    {
                        request.getValue().completeExceptionally(error);
                        requestsIterator.remove();
                    }
                }
            }
        }
    }
    
    private final class RequestTimeout implements Runnable
    {
        private final Object requestId;
        
        RequestTimeout(final Object requestId)
        {
            this.requestId = requestId;
        }
        
        @Override
        public void run()
        {
            CompletableFuture<Message> request = inflightRequests.remove(requestId);
            if (request != null)
            {
                request.completeExceptionally(new TimeoutException(String.format(Locale.US, "Request timed out, RequestId: %s", requestId)));
            }
        }
    }
}
