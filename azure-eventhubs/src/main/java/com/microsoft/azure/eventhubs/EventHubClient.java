/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import com.microsoft.aad.adal4j.AuthenticationCallback;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.AsymmetricKeyCredential;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.eventhubs.amqp.AmqpException;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.UnresolvedAddressException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Anchor class - all EventHub client operations STARTS here.
 *
 * @see EventHubClient#createFromConnectionString(String)
 */
public class EventHubClient extends ClientEntity implements IEventHubClient {
    private volatile boolean isSenderCreateStarted;
    public static final String DEFAULT_CONSUMER_GROUP_NAME = "$Default";

    /**
     * It will be truncated to 128 characters
     */
    public static String userAgent = null;

    private final String eventHubName;
    private final Object senderCreateSync;

    private MessagingFactory underlyingFactory;
    private MessageSender sender;
    private CompletableFuture<Void> createSender;

    private EventHubClient(final ConnectionStringBuilder connectionString) throws IOException, IllegalEntityException {
        this(connectionString.getEntityPath());
    }

    private EventHubClient(final String eventHubName) {
        super(StringUtil.getRandomString(), null);

        this.eventHubName = eventHubName;
        this.senderCreateSync = new Object();
    }

    /**
     * Synchronous version of {@link #createFromConnectionString(String)}.
     *
     * @param connectionString The connection string to be used. See {@link ConnectionStringBuilder} to construct a connectionString.
     * @return EventHubClient which can be used to create Senders and Receivers to EventHub
     * @throws EventHubException If Service Bus service encountered problems during connection creation.
     * @throws IOException         If the underlying Proton-J layer encounter network errors.
     */
    public static EventHubClient createFromConnectionStringSync(final String connectionString)
            throws EventHubException, IOException {
        return createFromConnectionStringSync(connectionString, null);
    }

    /**
     * Synchronous version of {@link #createFromConnectionString(String)}.
     *
     * @param connectionString The connection string to be used. See {@link ConnectionStringBuilder} to construct a connectionString.
     * @param retryPolicy      A custom {@link RetryPolicy} to be used when communicating with EventHub.
     * @return EventHubClient which can be used to create Senders and Receivers to EventHub
     * @throws EventHubException If Service Bus service encountered problems during connection creation.
     * @throws IOException         If the underlying Proton-J layer encounter network errors.
     */
    public static EventHubClient createFromConnectionStringSync(final String connectionString, final RetryPolicy retryPolicy)
            throws EventHubException, IOException {
        try {
            return createFromConnectionString(connectionString, retryPolicy).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Factory method to create an instance of {@link EventHubClient} using the supplied connectionString.
     * In a normal scenario (when re-direct is not enabled) - one EventHubClient instance maps to one Connection to the Azure ServiceBus EventHubs service.
     * <p>The {@link EventHubClient} created from this method creates a Sender instance internally, which is used by the {@link #send(EventData)} methods.
     *
     * @param connectionString The connection string to be used. See {@link ConnectionStringBuilder} to construct a connectionString.
     * @return EventHubClient which can be used to create Senders and Receivers to EventHub
     * @throws EventHubException If Service Bus service encountered problems during connection creation.
     * @throws IOException         If the underlying Proton-J layer encounter network errors.
     */
    public static CompletableFuture<EventHubClient> createFromConnectionString(final String connectionString)
            throws EventHubException, IOException {
        return createFromConnectionString(connectionString, null);
    }

    /**
     * Factory method to create an instance of {@link EventHubClient} using the supplied namespace endpoint address, eventhub name and authentication mechanism.
     * In a normal scenario (when re-direct is not enabled) - one EventHubClient instance maps to one Connection to the Azure ServiceBus EventHubs service.
     * <p>The {@link EventHubClient} created from this method creates a Sender instance internally, which is used by the {@link #send(EventData)} methods.
     *
     * @param endpointAddress namespace level endpoint. This needs to be in the format of scheme://fullyQualifiedServiceBusNamespaceEndpointName
     * @param eventHubName EventHub name
     * @param tokenProvider The {@link ITokenProvider} implementation to be used to authenticate
     * @return EventHubClient which can be used to create Senders and Receivers to EventHub
     * @throws EventHubException If the EventHubs service encountered problems during connection creation.
     * @throws IOException If the underlying Proton-J layer encounter network errors.
     */
    public static CompletableFuture<EventHubClient> create(
            final URI endpointAddress,
            final String eventHubName,
            final ITokenProvider tokenProvider) throws EventHubException, IOException {

        if (endpointAddress == null) {
            throw new IllegalArgumentException("endpointAddress cannot be null");
        }

        if (StringUtil.isNullOrWhiteSpace(eventHubName)) {
            throw new IllegalArgumentException("Specified EventHubName is illegal.");
        }

        if (tokenProvider == null) {
            throw new IllegalArgumentException("TokenProvider cannot be null");
        }

        final EventHubClient eventHubClient = new EventHubClient(eventHubName);

        return MessagingFactory.create(
                endpointAddress.getHost(),
                MessagingFactory.DefaultOperationTimeout,
                RetryPolicy.getDefault(),
                tokenProvider)
                .thenApplyAsync(new Function<MessagingFactory, EventHubClient>() {
                    @Override
                    public EventHubClient apply(MessagingFactory factory) {
                        eventHubClient.underlyingFactory = factory;
                        return eventHubClient;
                    }
                });
    }

    /**
     * Factory method to create an instance of {@link EventHubClient} using the supplied namespace endpoint address, eventhub name and authentication mechanism.
     * In a normal scenario (when re-direct is not enabled) - one EventHubClient instance maps to one Connection to the Azure ServiceBus EventHubs service.
     * <p>The {@link EventHubClient} created from this method creates a Sender instance internally, which is used by the {@link #send(EventData)} methods.
     *
     * @param endpointAddress  namespace level endpoint. This needs to be in the format of scheme://fullyQualifiedServiceBusNamespaceEndpointName
     * @param eventHubName EventHub name
     * @param authenticationContext The Azure Active Directory {@link AuthenticationContext}
     * @param clientCredential The Azure Active Directory {@link ClientCredential}
     * @return
     * @throws EventHubException If the EventHubs service encountered problems during connection creation.
     * @throws IOException If the underlying Proton-J layer encounter network errors.
     */
    public static CompletableFuture<EventHubClient> create(
            final URI endpointAddress,
            final String eventHubName,
            final AuthenticationContext authenticationContext,
            final ClientCredential clientCredential) throws EventHubException, IOException {

        if (authenticationContext == null) {
            throw new IllegalArgumentException("authenticationContext cannot be null");
        }

        if (clientCredential == null) {
            throw new IllegalArgumentException("clientCredential cannot be null");
        }

        return create(
                endpointAddress,
                eventHubName,
                new AzureActiveDirectoryTokenProvider(
                        authenticationContext,
                        new AzureActiveDirectoryTokenProvider.ITokenAcquirer() {
                            @Override
                            public Future<AuthenticationResult> acquireToken(
                                    final AuthenticationContext authenticationContext,
                                    final AuthenticationCallback authenticationCallback) {
                                return authenticationContext.acquireToken(
                                        AzureActiveDirectoryTokenProvider.EVENTHUBS_REGISTERED_AUDIENCE,
                                        clientCredential,
                                        authenticationCallback);
                            }
                        }));
    }

    /**
     * Factory method to create an instance of {@link EventHubClient} using the supplied namespace endpoint address, eventhub name and authentication mechanism.
     * In a normal scenario (when re-direct is not enabled) - one EventHubClient instance maps to one Connection to the Azure ServiceBus EventHubs service.
     * <p>The {@link EventHubClient} created from this method creates a Sender instance internally, which is used by the {@link #send(EventData)} methods.
     *
     * @param endpointAddress  namespace level endpoint. This needs to be in the format of scheme://fullyQualifiedServiceBusNamespaceEndpointName
     * @param eventHubName EventHub name
     * @param authenticationContext The Azure Active Directory {@link AuthenticationContext}
     * @param credential The Azure Active Directory {@link AsymmetricKeyCredential}
     * @return
     * @throws EventHubException If the EventHubs service encountered problems during connection creation.
     * @throws IOException If the underlying Proton-J layer encounter network errors.
     */
    public static CompletableFuture<EventHubClient> create(
            final URI endpointAddress,
            final String eventHubName,
            final AuthenticationContext authenticationContext,
            final AsymmetricKeyCredential credential) throws EventHubException, IOException {

        if (authenticationContext == null) {
            throw new IllegalArgumentException("authenticationContext cannot be null");
        }

        if (credential == null) {
            throw new IllegalArgumentException("credential cannot be null");
        }

        return create(
                endpointAddress,
                eventHubName,
                new AzureActiveDirectoryTokenProvider(
                        authenticationContext,
                        new AzureActiveDirectoryTokenProvider.ITokenAcquirer() {
                            @Override
                            public Future<AuthenticationResult> acquireToken(
                                    final AuthenticationContext authenticationContext,
                                    final AuthenticationCallback authenticationCallback) {
                                return authenticationContext.acquireToken(
                                        AzureActiveDirectoryTokenProvider.EVENTHUBS_REGISTERED_AUDIENCE,
                                        credential,
                                        authenticationCallback);
                            }
                        }));
    }

    /**
     * Factory method to create an instance of {@link EventHubClient} using the supplied namespace endpoint address, eventhub name and authentication mechanism.
     * In a normal scenario (when re-direct is not enabled) - one EventHubClient instance maps to one Connection to the Azure ServiceBus EventHubs service.
     * <p>The {@link EventHubClient} created from this method creates a Sender instance internally, which is used by the {@link #send(EventData)} methods.
     *
     * @param endpointAddress  namespace level endpoint. This needs to be in the format of scheme://fullyQualifiedServiceBusNamespaceEndpointName
     * @param eventHubName EventHub name
     * @return
     * @throws EventHubException If the EventHubs service encountered problems during connection creation.
     * @throws IOException If the underlying Proton-J layer encounter network errors.
     */
    public static CompletableFuture<EventHubClient> createWithManagedServiceIdentity(
            final URI endpointAddress,
            final String eventHubName) throws EventHubException, IOException {

        return create(
                endpointAddress,
                eventHubName,
                new ManagedServiceIdentityTokenProvider());
    }

    /**
     * Factory method to create an instance of {@link EventHubClient} using the supplied connectionString.
     * In a normal scenario (when re-direct is not enabled) - one EventHubClient instance maps to one Connection to the Azure ServiceBus EventHubs service.
     * <p>The {@link EventHubClient} created from this method creates a Sender instance internally, which is used by the {@link #send(EventData)} methods.
     *
     * @param connectionString The connection string to be used. See {@link ConnectionStringBuilder} to construct a connectionString.
     * @param retryPolicy      A custom {@link RetryPolicy} to be used when communicating with EventHub.
     * @return EventHubClient which can be used to create Senders and Receivers to EventHub
     * @throws EventHubException If Service Bus service encountered problems during connection creation.
     * @throws IOException         If the underlying Proton-J layer encounter network errors.
     */
    public static CompletableFuture<EventHubClient> createFromConnectionString(final String connectionString, final RetryPolicy retryPolicy)
            throws EventHubException, IOException {
        final ConnectionStringBuilder connStr = new ConnectionStringBuilder(connectionString);
        final EventHubClient eventHubClient = new EventHubClient(connStr);

        return MessagingFactory.createFromConnectionString(connectionString.toString(), retryPolicy)
                .thenApplyAsync(new Function<MessagingFactory, EventHubClient>() {
                    @Override
                    public EventHubClient apply(MessagingFactory factory) {
                        eventHubClient.underlyingFactory = factory;
                        return eventHubClient;
                    }
                });
    }

    /**
     * Creates an Empty Collection of {@link EventData}.
     * The same partitionKey must be used while sending these events using {@link EventHubClient#send(EventDataBatch)}.
     *
     * @param options see {@link BatchOptions} for more details
     * @return the empty {@link EventDataBatch}, after negotiating maximum message size with EventHubs service
     * @throws EventHubException if the Microsoft Azure Event Hubs service encountered problems during the operation.
     */
    public final EventDataBatch createBatch(BatchOptions options) throws EventHubException {
        try {
            int maxSize = this.createInternalSender().thenApplyAsync((aVoid) -> this.sender.getMaxMessageSize()).get();
            if (options.maxMessageSize == null) {
                return new EventDataBatch(maxSize, options.partitionKey);
            }

            if (options.maxMessageSize > maxSize) {
                throw new IllegalArgumentException("The maxMessageSize set in BatchOptions is too large. You set a maxMessageSize of " +
                    options.maxMessageSize + ". The maximum allowed size is " + maxSize + ".");
            }

            return new EventDataBatch(options.maxMessageSize, options.partitionKey);
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert thread's interrupted status
                Thread.currentThread().interrupt();
            }

            final Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Creates an Empty Collection of {@link EventData}.
     * The same partitionKey must be used while sending these events using {@link EventHubClient#send(EventDataBatch)}.
     *
     * @return the empty {@link EventDataBatch}, after negotiating maximum message size with EventHubs service
     * @throws EventHubException if the Microsoft Azure Event Hubs service encountered problems during the operation.
     */
    public final EventDataBatch createBatch() throws EventHubException {
        return this.createBatch(new BatchOptions());
    }

    /**
     * Synchronous version of {@link #send(EventData)}.
     *
     * @param data the {@link EventData} to be sent.
     * @throws PayloadSizeExceededException if the total size of the {@link EventData} exceeds a predefined limit set by the service. Default is 256k bytes.
     * @throws EventHubException          if Service Bus service encountered problems during the operation.
     * @throws UnresolvedAddressException   if there are Client to Service network connectivity issues, if the Azure DNS resolution of the ServiceBus Namespace fails (ex: namespace deleted etc.)
     */
    @Override
    public final void sendSync(final EventData data) throws EventHubException {
        try {
            this.send(data).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Send {@link EventData} to EventHub. The sent {@link EventData} will land on any arbitrarily chosen EventHubs partition.
     * <p>There are 3 ways to send to EventHubs, each exposed as a method (along with its sendBatch overload):
     * <ul>
     * <li>	{@link #send(EventData)}, {@link #send(Iterable)}, or {@link #send(EventDataBatch)}
     * <li>	{@link #send(EventData, String)} or {@link #send(Iterable, String)}
     * <li>	{@link PartitionSender#send(EventData)}, {@link PartitionSender#send(Iterable)}, or {@link PartitionSender#send(EventDataBatch)}
     * </ul>
     * <p>Use this method to Send, if:
     * <pre>
     * a)  the send({@link EventData}) operation should be highly available and
     * b)  the data needs to be evenly distributed among all partitions; exception being, when a subset of partitions are unavailable
     * </pre>
     * <p>
     * {@link #send(EventData)} send's the {@link EventData} to a Service Gateway, which in-turn will forward the {@link EventData} to one of the EventHubs' partitions. Here's the message forwarding algorithm:
     * <pre>
     * i.  Forward the {@link EventData}'s to EventHub partitions, by equally distributing the data among all partitions (ex: Round-robin the {@link EventData}'s to all EventHubs' partitions)
     * ii. If one of the EventHub partitions is unavailable for a moment, the Service Gateway will automatically detect it and forward the message to another available partition - making the Send operation highly-available.
     * </pre>
     *
     * @param data the {@link EventData} to be sent.
     * @return a CompletableFuture that can be completed when the send operations is done..
     * @see #send(EventData, String)
     * @see PartitionSender#send(EventData)
     */
    @Override
    public final CompletableFuture<Void> send(final EventData data) {
        if (data == null) {
            throw new IllegalArgumentException("EventData cannot be empty.");
        }

        return this.createInternalSender().thenComposeAsync(new Function<Void, CompletableFuture<Void>>() {
            @Override
            public CompletableFuture<Void> apply(Void voidArg) {
                return EventHubClient.this.sender.send(data.toAmqpMessage());
            }
        });
    }

    /**
     * Synchronous version of {@link #send(Iterable)}.
     *
     * @param eventDatas batch of events to send to EventHub
     * @throws PayloadSizeExceededException if the total size of the {@link EventData} exceeds a pre-defined limit set by the service. Default is 256k bytes.
     * @throws EventHubException          if Service Bus service encountered problems during the operation.
     * @throws UnresolvedAddressException   if there are Client to Service network connectivity issues, if the Azure DNS resolution of the ServiceBus Namespace fails (ex: namespace deleted etc.)
     */
    @Override
    public final void sendSync(final Iterable<EventData> eventDatas) throws EventHubException {
        try {
            this.send(eventDatas).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

   /**
     * Send a batch of {@link EventData} to EventHub. The sent {@link EventData} will land on any arbitrarily chosen EventHubs partition.
     * This is the most recommended way to Send to EventHubs.
     * <p>There are 3 ways to send to EventHubs, to understand this particular type of Send refer to the overload {@link #send(EventData)}, which is used to send single {@link EventData}.
     * Use this overload versus {@link #send(EventData)}, if you need to send a batch of {@link EventData}.
     * <p> Sending a batch of {@link EventData}'s is useful in the following cases:
     * <pre>
     * i.	Efficient send - sending a batch of {@link EventData} maximizes the overall throughput by optimally using the number of sessions created to EventHubs' service.
     * ii.	Send multiple {@link EventData}'s in a Transaction. To achieve ACID properties, the Gateway Service will forward all {@link EventData}'s in the batch to a single EventHubs' partition.
     * </pre>
     * <p>
     * Sample code (sample uses sync version of the api but concept are identical):
     * <pre>
     * Gson gson = new GsonBuilder().create();
     * EventHubClient client = EventHubClient.createFromConnectionStringSync("__connection__");
     *
     * while (true)
     * {
     *     LinkedList{@literal<}EventData{@literal>} events = new LinkedList{@literal<}EventData{@literal>}();}
     *     for (int count = 1; count {@literal<} 11; count++)
     *     {
     *         PayloadEvent payload = new PayloadEvent(count);
     *         byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
     *         EventData sendEvent = new EventData(payloadBytes);
     *         Map{@literal<}String, String{@literal>} applicationProperties = new HashMap{@literal<}String, String{@literal>}();
     *         applicationProperties.put("from", "javaClient");
     *         sendEvent.setProperties(applicationProperties);
     *         events.add(sendEvent);
     *     }
     *
     *     client.sendSync(events);
     *     System.out.println(String.format("Sent Batch... Size: %s", events.size()));
     * }
     * </pre>
     * <p> for Exceptions refer to {@link #sendSync(Iterable)}
     *
     * @param eventDatas batch of events to send to EventHub
     * @return a CompletableFuture that can be completed when the send operations is done..
     * @see #send(EventData, String)
     * @see PartitionSender#send(EventData)
     */
    @Override
    public final CompletableFuture<Void> send(final Iterable<EventData> eventDatas) {
        if (eventDatas == null || IteratorUtil.sizeEquals(eventDatas, 0)) {
            throw new IllegalArgumentException("Empty batch of EventData cannot be sent.");
        }

        return this.createInternalSender().thenComposeAsync(new Function<Void, CompletableFuture<Void>>() {
            @Override
            public CompletableFuture<Void> apply(Void voidArg) {
                return EventHubClient.this.sender.send(EventDataUtil.toAmqpMessages(eventDatas));
            }
        });
    }

    /**
     * Synchronous version of {@link #send(EventDataBatch)}.
     *
     * @param eventDatas EventDataBatch to send to EventHub
     * @throws EventHubException        if Service Bus service encountered problems during the operation.
     */
    @Override
    public final void sendSync(final EventDataBatch eventDatas) throws EventHubException {
        try {
            this.send(eventDatas).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Send {@link EventDataBatch} to EventHub. The sent {@link EventDataBatch} will land according the partition key
     * set in the {@link EventDataBatch}. If a partition key is not set, then we will Round-robin the {@link EventData}'s
     * to all EventHubs' partitions.
     *
     * @param eventDatas EventDataBatch to send to EventHub
     * @return a CompleteableFuture that can be completed when the send operations are done
     * @see #send(Iterable)
     * @see EventDataBatch
     */
    @Override
    public final CompletableFuture<Void> send(final EventDataBatch eventDatas) {
        if (eventDatas == null || Integer.compare(eventDatas.getSize(), 0) == 0) {
            throw new IllegalArgumentException("Empty batch of EventData cannot be sent.");
        }

        return this.send(eventDatas.getInternalIterable(), eventDatas.getPartitionKey());
    }

    /**
     * Synchronous version of {@link #send(EventData, String)}.
     *
     * @param eventData    the {@link EventData} to be sent.
     * @param partitionKey the partitionKey will be hash'ed to determine the partitionId to send the eventData to. On the Received message this can be accessed at {@link EventData.SystemProperties#getPartitionKey()}
     * @throws PayloadSizeExceededException if the total size of the {@link EventData} exceeds a pre-defined limit set by the service. Default is 256k bytes.
     * @throws EventHubException          if Service Bus service encountered problems during the operation.
     */
    @Override
    public final void sendSync(final EventData eventData, final String partitionKey) throws EventHubException {
        try {
            this.send(eventData, partitionKey).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Send an '{@link EventData} with a partitionKey' to EventHub. All {@link EventData}'s with a partitionKey are guaranteed to land on the same partition.
     * This send pattern emphasize data correlation over general availability and latency.
     * <p>
     * There are 3 ways to send to EventHubs, each exposed as a method (along with its sendBatch overload):
     * <pre>
     * i.   {@link #send(EventData)} or {@link #send(Iterable)}
     * ii.  {@link #send(EventData, String)} or {@link #send(Iterable, String)}
     * iii. {@link PartitionSender#send(EventData)} or {@link PartitionSender#send(Iterable)}
     * </pre>
     * <p>
     * Use this type of Send, if:
     * <pre>
     * i.  There is a need for correlation of events based on Sender instance; The sender can generate a UniqueId and set it as partitionKey - which on the received Message can be used for correlation
     * ii. The client wants to take control of distribution of data across partitions.
     * </pre>
     * <p>
     * Multiple PartitionKey's could be mapped to one Partition. EventHubs service uses a proprietary Hash algorithm to map the PartitionKey to a PartitionId.
     * Using this type of Send (Sending using a specific partitionKey), could sometimes result in partitions which are not evenly distributed.
     *
     * @param eventData    the {@link EventData} to be sent.
     * @param partitionKey the partitionKey will be hash'ed to determine the partitionId to send the eventData to. On the Received message this can be accessed at {@link EventData.SystemProperties#getPartitionKey()}
     * @return a CompletableFuture that can be completed when the send operations is done..
     * @see #send(EventData)
     * @see PartitionSender#send(EventData)
     */
    @Override
    public final CompletableFuture<Void> send(final EventData eventData, final String partitionKey) {
        if (eventData == null) {
            throw new IllegalArgumentException("EventData cannot be null.");
        }

        if (partitionKey == null) {
            throw new IllegalArgumentException("partitionKey cannot be null");
        }

        return this.createInternalSender().thenComposeAsync(new Function<Void, CompletableFuture<Void>>() {
            @Override
            public CompletableFuture<Void> apply(Void voidArg) {
                return EventHubClient.this.sender.send(eventData.toAmqpMessage(partitionKey));
            }
        });
    }

    /**
     * Synchronous version of {@link #send(Iterable, String)}.
     *
     * @param eventDatas   the batch of events to send to EventHub
     * @param partitionKey the partitionKey will be hash'ed to determine the partitionId to send the eventData to. On the Received message this can be accessed at {@link EventData.SystemProperties#getPartitionKey()}
     * @throws PayloadSizeExceededException if the total size of the {@link EventData} exceeds a pre-defined limit set by the service. Default is 256k bytes.
     * @throws EventHubException          if Service Bus service encountered problems during the operation.
     * @throws UnresolvedAddressException   if there are Client to Service network connectivity issues, if the Azure DNS resolution of the ServiceBus Namespace fails (ex: namespace deleted etc.)
     */
    @Override
    public final void sendSync(final Iterable<EventData> eventDatas, final String partitionKey)
            throws EventHubException {
        try {
            this.send(eventDatas, partitionKey).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Send a 'batch of {@link EventData} with the same partitionKey' to EventHub. All {@link EventData}'s with a partitionKey are guaranteed to land on the same partition.
     * Multiple PartitionKey's will be mapped to one Partition.
     * <p>There are 3 ways to send to EventHubs, to understand this particular type of Send refer to the overload {@link #send(EventData, String)}, which is the same type of Send and is used to send single {@link EventData}.
     * <p>Sending a batch of {@link EventData}'s is useful in the following cases:
     * <pre>
     * i.	Efficient send - sending a batch of {@link EventData} maximizes the overall throughput by optimally using the number of sessions created to EventHubs service.
     * ii.	Send multiple events in One Transaction. This is the reason why all events sent in a batch needs to have same partitionKey (so that they are sent to one partition only).
     * </pre>
     *
     * @param eventDatas   the batch of events to send to EventHub
     * @param partitionKey the partitionKey will be hash'ed to determine the partitionId to send the eventData to. On the Received message this can be accessed at {@link EventData.SystemProperties#getPartitionKey()}
     * @return a CompletableFuture that can be completed when the send operations is done..
     * @see #send(EventData)
     * @see PartitionSender#send(EventData)
     */
    @Override
    public final CompletableFuture<Void> send(final Iterable<EventData> eventDatas, final String partitionKey) {
        if (eventDatas == null || IteratorUtil.sizeEquals(eventDatas, 0)) {
            throw new IllegalArgumentException("Empty batch of EventData cannot be sent.");
        }

        if (partitionKey == null) {
            throw new IllegalArgumentException("partitionKey cannot be null");
        }

        if (partitionKey.length() > ClientConstants.MAX_PARTITION_KEY_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(Locale.US, "PartitionKey exceeds the maximum allowed length of partitionKey: {0}", ClientConstants.MAX_PARTITION_KEY_LENGTH));
        }

        return this.createInternalSender().thenComposeAsync(new Function<Void, CompletableFuture<Void>>() {
            @Override
            public CompletableFuture<Void> apply(Void voidArg) {
                return EventHubClient.this.sender.send(EventDataUtil.toAmqpMessages(eventDatas, partitionKey));
            }
        });
    }

    /**
     * Synchronous version of {@link #createPartitionSender(String)}.
     *
     * @param partitionId partitionId of EventHub to send the {@link EventData}'s to
     * @return PartitionSender which can be used to send events to a specific partition.
     * @throws EventHubException if Service Bus service encountered problems during connection creation.
     */
    @Override
    public final PartitionSender createPartitionSenderSync(final String partitionId)
            throws EventHubException, IllegalArgumentException {
        try {
            return this.createPartitionSender(partitionId).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create a {@link PartitionSender} which can publish {@link EventData}'s directly to a specific EventHub partition (sender type iii. in the below list).
     * <p>
     * There are 3 patterns/ways to send to EventHubs:
     * <pre>
     * i.   {@link #send(EventData)} or {@link #send(Iterable)}
     * ii.  {@link #send(EventData, String)} or {@link #send(Iterable, String)}
     * iii. {@link PartitionSender#send(EventData)} or {@link PartitionSender#send(Iterable)}
     * </pre>
     *
     * @param partitionId partitionId of EventHub to send the {@link EventData}'s to
     * @return a CompletableFuture that would result in a PartitionSender when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during connection creation.
     * @see PartitionSender
     */
    @Override
    public final CompletableFuture<PartitionSender> createPartitionSender(final String partitionId)
            throws EventHubException {
        return PartitionSender.Create(this.underlyingFactory, this.eventHubName, partitionId);
    }

    /**
     * Synchronous version of {@link #createReceiver(String, String, String)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createReceiverSync(final String consumerGroupName, final String partitionId, final String startingOffset)
            throws EventHubException {
        try {
            return this.createReceiver(consumerGroupName, partitionId, startingOffset).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * The receiver is created for a specific EventHub partition from the specific consumer group.
     * <p>NOTE: There can be a maximum number of receivers that can run in parallel per ConsumerGroup per Partition.
     * The limit is enforced by the Event Hub service - current limit is 5 receivers in parallel. Having multiple receivers
     * reading from offsets that are far apart on the same consumer group / partition combo will have significant performance Impact.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @return a CompletableFuture that would result in a PartitionReceiver instance when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createReceiver(final String consumerGroupName, final String partitionId, final String startingOffset)
            throws EventHubException {
        return this.createReceiver(consumerGroupName, partitionId, startingOffset, false);
    }

    /**
     * Synchronous version of {@link #createReceiver(String, String, String, boolean)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param offsetInclusive   if set to true, the startingOffset is treated as an inclusive offset - meaning the first event returned is the one that has the starting offset. Normally first event returned is the event after the starting offset.
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createReceiverSync(final String consumerGroupName, final String partitionId, final String startingOffset, boolean offsetInclusive)
            throws EventHubException {
        try {
            return this.createReceiver(consumerGroupName, partitionId, startingOffset, offsetInclusive).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }

        }
    }

    /**
     * Create the EventHub receiver with given partition id and start receiving from the specified starting offset.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param offsetInclusive   if set to true, the startingOffset is treated as an inclusive offset - meaning the first event returned is the one that has the starting offset. Normally first event returned is the event after the starting offset.
     * @return a CompletableFuture that would result in a PartitionReceiver instance when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createReceiver(final String consumerGroupName, final String partitionId, final String startingOffset, boolean offsetInclusive)
            throws EventHubException {
        return this.createReceiver(consumerGroupName, partitionId, startingOffset, offsetInclusive, null);
    }

    /**
     * Synchronous version of {@link #createReceiver(String, String, Instant)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param dateTime          the date time instant that receive operations will start receive events from. Events received will have {@link EventData.SystemProperties#getEnqueuedTime()} later than this Instant.
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createReceiverSync(final String consumerGroupName, final String partitionId, final Instant dateTime)
            throws EventHubException {
        try {
            return this.createReceiver(consumerGroupName, partitionId, dateTime).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create the EventHub receiver with given partition id and start receiving from the specified starting offset.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param dateTime          the date time instant that receive operations will start receive events from. Events received will have {@link EventData.SystemProperties#getEnqueuedTime()} later than this Instant.
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createReceiver(final String consumerGroupName, final String partitionId, final Instant dateTime)
            throws EventHubException {
        return this.createReceiver(consumerGroupName, partitionId, dateTime, null);
    }

    /**
     * Synchronous version of {@link #createReceiver(String, String, String)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createReceiverSync(final String consumerGroupName, final String partitionId, final String startingOffset, final ReceiverOptions receiverOptions)
            throws EventHubException {
        try {
            return this.createReceiver(consumerGroupName, partitionId, startingOffset, receiverOptions).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * The receiver is created for a specific EventHub partition from the specific consumer group.
     * <p>NOTE: There can be a maximum number of receivers that can run in parallel per ConsumerGroup per Partition.
     * The limit is enforced by the Event Hub service - current limit is 5 receivers in parallel. Having multiple receivers
     * reading from offsets that are far apart on the same consumer group / partition combo will have significant performance Impact.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return a CompletableFuture that would result in a PartitionReceiver instance when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createReceiver(final String consumerGroupName, final String partitionId, final String startingOffset, final ReceiverOptions receiverOptions)
            throws EventHubException {
        return this.createReceiver(consumerGroupName, partitionId, startingOffset, false, receiverOptions);
    }

    /**
     * Synchronous version of {@link #createReceiver(String, String, String, boolean)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param offsetInclusive   if set to true, the startingOffset is treated as an inclusive offset - meaning the first event returned is the one that has the starting offset. Normally first event returned is the event after the starting offset.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createReceiverSync(final String consumerGroupName, final String partitionId, final String startingOffset, boolean offsetInclusive, final ReceiverOptions receiverOptions)
            throws EventHubException {
        try {
            return this.createReceiver(consumerGroupName, partitionId, startingOffset, offsetInclusive, receiverOptions).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create the EventHub receiver with given partition id and start receiving from the specified starting offset.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param offsetInclusive   if set to true, the startingOffset is treated as an inclusive offset - meaning the first event returned is the one that has the starting offset. Normally first event returned is the event after the starting offset.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return a CompletableFuture that would result in a PartitionReceiver instance when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createReceiver(final String consumerGroupName, final String partitionId, final String startingOffset, boolean offsetInclusive, final ReceiverOptions receiverOptions)
            throws EventHubException {
        return PartitionReceiver.create(this.underlyingFactory, this.eventHubName, consumerGroupName, partitionId, startingOffset, offsetInclusive, null, PartitionReceiver.NULL_EPOCH, false, receiverOptions);
    }

    /**
     * Synchronous version of {@link #createReceiver(String, String, Instant)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param dateTime          the date time instant that receive operations will start receive events from. Events received will have {@link EventData.SystemProperties#getEnqueuedTime()} later than this Instant.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createReceiverSync(final String consumerGroupName, final String partitionId, final Instant dateTime, final ReceiverOptions receiverOptions)
            throws EventHubException {
        try {
            return this.createReceiver(consumerGroupName, partitionId, dateTime, receiverOptions).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create the EventHub receiver with given partition id and start receiving from the specified starting offset.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param dateTime          the date time instant that receive operations will start receive events from. Events received will have {@link EventData.SystemProperties#getEnqueuedTime()} later than this Instant.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createReceiver(final String consumerGroupName, final String partitionId, final Instant dateTime, final ReceiverOptions receiverOptions)
            throws EventHubException {
        return PartitionReceiver.create(this.underlyingFactory, this.eventHubName, consumerGroupName, partitionId, null, false, dateTime, PartitionReceiver.NULL_EPOCH, false, receiverOptions);
    }

    /**
     * Synchronous version of {@link #createEpochReceiver(String, String, String, long)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createEpochReceiverSync(final String consumerGroupName, final String partitionId, final String startingOffset, final long epoch)
            throws EventHubException {
        try {
            return this.createEpochReceiver(consumerGroupName, partitionId, startingOffset, epoch).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create a Epoch based EventHub receiver with given partition id and start receiving from the beginning of the partition stream.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     * <p>
     * It is important to pay attention to the following when creating epoch based receiver:
     * <ul>
     * <li> Ownership enforcement - Once you created an epoch based receiver, you cannot create a non-epoch receiver to the same consumerGroup-Partition combo until all receivers to the combo are closed.
     * <li> Ownership stealing - If a receiver with higher epoch value is created for a consumerGroup-Partition combo, any older epoch receiver to that combo will be force closed.
     * <li> Any receiver closed due to lost of ownership to a consumerGroup-Partition combo will get ReceiverDisconnectedException for all operations from that receiver.
     * </ul>
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     * @see ReceiverDisconnectedException
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createEpochReceiver(final String consumerGroupName, final String partitionId, final String startingOffset, final long epoch)
            throws EventHubException {
        return this.createEpochReceiver(consumerGroupName, partitionId, startingOffset, false, epoch);
    }

    /**
     * Synchronous version of {@link #createEpochReceiver(String, String, String, boolean, long)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param offsetInclusive   if set to true, the startingOffset is treated as an inclusive offset - meaning the first event returned is the one that has the starting offset. Normally first event returned is the event after the starting offset.
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createEpochReceiverSync(final String consumerGroupName, final String partitionId, final String startingOffset, boolean offsetInclusive, final long epoch)
            throws EventHubException {
        try {
            return this.createEpochReceiver(consumerGroupName, partitionId, startingOffset, offsetInclusive, epoch).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create a Epoch based EventHub receiver with given partition id and start receiving from the beginning of the partition stream.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     * <p>
     * It is important to pay attention to the following when creating epoch based receiver:
     * <ul>
     * <li> Ownership enforcement - Once you created an epoch based receiver, you cannot create a non-epoch receiver to the same consumerGroup-Partition combo until all receivers to the combo are closed.
     * <li> Ownership stealing - If a receiver with higher epoch value is created for a consumerGroup-Partition combo, any older epoch receiver to that combo will be force closed.
     * <li> Any receiver closed due to lost of ownership to a consumerGroup-Partition combo will get ReceiverDisconnectedException for all operations from that receiver.
     * </ul>
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param offsetInclusive   if set to true, the startingOffset is treated as an inclusive offset - meaning the first event returned is the one that has the starting offset. Normally first event returned is the event after the starting offset.
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     * @see ReceiverDisconnectedException
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createEpochReceiver(final String consumerGroupName, final String partitionId, final String startingOffset, boolean offsetInclusive, final long epoch)
            throws EventHubException {
        return this.createEpochReceiver(consumerGroupName, partitionId, startingOffset, offsetInclusive, epoch, null);
    }

    /**
     * Synchronous version of {@link #createEpochReceiver(String, String, Instant, long)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param dateTime          the date time instant that receive operations will start receive events from. Events received will have {@link EventData.SystemProperties#getEnqueuedTime()} later than this Instant.
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createEpochReceiverSync(final String consumerGroupName, final String partitionId, final Instant dateTime, final long epoch)
            throws EventHubException {
        try {
            return this.createEpochReceiver(consumerGroupName, partitionId, dateTime, epoch).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create a Epoch based EventHub receiver with given partition id and start receiving from the beginning of the partition stream.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     * <p>
     * It is important to pay attention to the following when creating epoch based receiver:
     * <ul>
     * <li> Ownership enforcement - Once you created an epoch based receiver, you cannot create a non-epoch receiver to the same consumerGroup-Partition combo until all receivers to the combo are closed.
     * <li> Ownership stealing - If a receiver with higher epoch value is created for a consumerGroup-Partition combo, any older epoch receiver to that combo will be force closed.
     * <li> Any receiver closed due to lost of ownership to a consumerGroup-Partition combo will get ReceiverDisconnectedException for all operations from that receiver.
     * </ul>
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param dateTime          the date time instant that receive operations will start receive events from. Events received will have {@link EventData.SystemProperties#getEnqueuedTime()} later than this Instant.
     * @param epoch             a unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     * @see ReceiverDisconnectedException
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createEpochReceiver(final String consumerGroupName, final String partitionId, final Instant dateTime, final long epoch)
            throws EventHubException {
        return this.createEpochReceiver(consumerGroupName, partitionId, dateTime, epoch, null);
    }

    /**
     * Synchronous version of {@link #createEpochReceiver(String, String, String, long)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createEpochReceiverSync(final String consumerGroupName, final String partitionId, final String startingOffset, final long epoch, final ReceiverOptions receiverOptions)
            throws EventHubException {
        try {
            return this.createEpochReceiver(consumerGroupName, partitionId, startingOffset, epoch, receiverOptions).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create a Epoch based EventHub receiver with given partition id and start receiving from the beginning of the partition stream.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     * <p>
     * It is important to pay attention to the following when creating epoch based receiver:
     * <ul>
     * <li> Ownership enforcement - Once you created an epoch based receiver, you cannot create a non-epoch receiver to the same consumerGroup-Partition combo until all receivers to the combo are closed.
     * <li> Ownership stealing - If a receiver with higher epoch value is created for a consumerGroup-Partition combo, any older epoch receiver to that combo will be force closed.
     * <li> Any receiver closed due to lost of ownership to a consumerGroup-Partition combo will get ReceiverDisconnectedException for all operations from that receiver.
     * </ul>
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     * @see ReceiverDisconnectedException
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createEpochReceiver(final String consumerGroupName, final String partitionId, final String startingOffset, final long epoch, final ReceiverOptions receiverOptions)
            throws EventHubException {
        return this.createEpochReceiver(consumerGroupName, partitionId, startingOffset, false, epoch, receiverOptions);
    }

    /**
     * Synchronous version of {@link #createEpochReceiver(String, String, String, boolean, long)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param offsetInclusive   if set to true, the startingOffset is treated as an inclusive offset - meaning the first event returned is the one that has the starting offset. Normally first event returned is the event after the starting offset.
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createEpochReceiverSync(final String consumerGroupName, final String partitionId, final String startingOffset, boolean offsetInclusive, final long epoch, final ReceiverOptions receiverOptions)
            throws EventHubException {
        try {
            return this.createEpochReceiver(consumerGroupName, partitionId, startingOffset, offsetInclusive, epoch, receiverOptions).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create a Epoch based EventHub receiver with given partition id and start receiving from the beginning of the partition stream.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     * <p>
     * It is important to pay attention to the following when creating epoch based receiver:
     * <ul>
     * <li> Ownership enforcement - Once you created an epoch based receiver, you cannot create a non-epoch receiver to the same consumerGroup-Partition combo until all receivers to the combo are closed.
     * <li> Ownership stealing - If a receiver with higher epoch value is created for a consumerGroup-Partition combo, any older epoch receiver to that combo will be force closed.
     * <li> Any receiver closed due to lost of ownership to a consumerGroup-Partition combo will get ReceiverDisconnectedException for all operations from that receiver.
     * </ul>
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param startingOffset    the offset to start receiving the events from. To receive from start of the stream use: {@link PartitionReceiver#START_OF_STREAM}
     * @param offsetInclusive   if set to true, the startingOffset is treated as an inclusive offset - meaning the first event returned is the one that has the starting offset. Normally first event returned is the event after the starting offset.
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     * @see ReceiverDisconnectedException
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createEpochReceiver(final String consumerGroupName, final String partitionId, final String startingOffset, boolean offsetInclusive, final long epoch, final ReceiverOptions receiverOptions)
            throws EventHubException {
        return PartitionReceiver.create(this.underlyingFactory, this.eventHubName, consumerGroupName, partitionId, startingOffset, offsetInclusive, null, epoch, true, receiverOptions);
    }

    /**
     * Synchronous version of {@link #createEpochReceiver(String, String, Instant, long)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param dateTime          the date time instant that receive operations will start receive events from. Events received will have {@link EventData.SystemProperties#getEnqueuedTime()} later than this Instant.
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    @Override
    public final PartitionReceiver createEpochReceiverSync(final String consumerGroupName, final String partitionId, final Instant dateTime, final long epoch, final ReceiverOptions receiverOptions)
            throws EventHubException {
        try {
            return this.createEpochReceiver(consumerGroupName, partitionId, dateTime, epoch, receiverOptions).get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new RuntimeException(exception);
            }
        }
    }

    /**
     * Create a Epoch based EventHub receiver with given partition id and start receiving from the beginning of the partition stream.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     * <p>
     * It is important to pay attention to the following when creating epoch based receiver:
     * <ul>
     * <li> Ownership enforcement - Once you created an epoch based receiver, you cannot create a non-epoch receiver to the same consumerGroup-Partition combo until all receivers to the combo are closed.
     * <li> Ownership stealing - If a receiver with higher epoch value is created for a consumerGroup-Partition combo, any older epoch receiver to that combo will be force closed.
     * <li> Any receiver closed due to lost of ownership to a consumerGroup-Partition combo will get ReceiverDisconnectedException for all operations from that receiver.
     * </ul>
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param dateTime          the date time instant that receive operations will start receive events from. Events received will have {@link EventData.SystemProperties#getEnqueuedTime()} later than this Instant.
     * @param epoch             a unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     * @see ReceiverDisconnectedException
     */
    @Override
    public final CompletableFuture<PartitionReceiver> createEpochReceiver(final String consumerGroupName, final String partitionId, final Instant dateTime, final long epoch, final ReceiverOptions receiverOptions)
            throws EventHubException {
        return PartitionReceiver.create(this.underlyingFactory, this.eventHubName, consumerGroupName, partitionId, null, false, dateTime, epoch, true, receiverOptions);
    }

    @Override
    public CompletableFuture<Void> onClose() {
        if (this.underlyingFactory != null) {
            synchronized (this.senderCreateSync) {
                final CompletableFuture<Void> internalSenderClose = this.sender != null
                        ? this.sender.close().thenComposeAsync(new Function<Void, CompletableFuture<Void>>() {
                    @Override
                    public CompletableFuture<Void> apply(Void voidArg) {
                        return EventHubClient.this.underlyingFactory.close();
                    }
                })
                        : this.underlyingFactory.close();

                return internalSenderClose;
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> createInternalSender() {
        if (!this.isSenderCreateStarted) {
            synchronized (this.senderCreateSync) {
                if (!this.isSenderCreateStarted) {
                    this.createSender = MessageSender.create(this.underlyingFactory, StringUtil.getRandomString(), this.eventHubName)
                            .thenAcceptAsync(new Consumer<MessageSender>() {
                                public void accept(MessageSender a) {
                                    EventHubClient.this.sender = a;
                                }
                            });

                    this.isSenderCreateStarted = true;
                }
            }
        }

        return this.createSender;
    }
    
    /**
     * Retrieves general information about an event hub (see {@link EventHubRuntimeInformation} for details).
     * Retries until it reaches the operation timeout, then either rethrows the last error if available or
     * returns null to indicate timeout.
     * 
     * @return CompletableFuture which returns an EventHubRuntimeInformation on success, or null on timeout.
     */
    @Override
    public CompletableFuture<EventHubRuntimeInformation> getRuntimeInformation() {
    	CompletableFuture<EventHubRuntimeInformation> future1 = null;
    	
    	throwIfClosed();

    	Map<String, String> request = new HashMap<String, String>();
        request.put(ClientConstants.MANAGEMENT_ENTITY_TYPE_KEY, ClientConstants.MANAGEMENT_EVENTHUB_ENTITY_TYPE);
        request.put(ClientConstants.MANAGEMENT_ENTITY_NAME_KEY, this.eventHubName);
        request.put(ClientConstants.MANAGEMENT_OPERATION_KEY, ClientConstants.READ_OPERATION_VALUE);
        future1 = this.<EventHubRuntimeInformation>addManagementToken(request);

        if (future1 == null) {
	        future1 = managementWithRetry(request).thenComposeAsync(new Function<Map<String, Object>, CompletableFuture<EventHubRuntimeInformation>>() {
				@Override
				public CompletableFuture<EventHubRuntimeInformation> apply(Map<String, Object> rawdata) {
			        CompletableFuture<EventHubRuntimeInformation> future2 = new CompletableFuture<EventHubRuntimeInformation>();
					future2.complete(new EventHubRuntimeInformation(
							(String)rawdata.get(ClientConstants.MANAGEMENT_ENTITY_NAME_KEY),
							((Date)rawdata.get(ClientConstants.MANAGEMENT_RESULT_CREATED_AT)).toInstant(),
							(int)rawdata.get(ClientConstants.MANAGEMENT_RESULT_PARTITION_COUNT),
							(String[])rawdata.get(ClientConstants.MANAGEMENT_RESULT_PARTITION_IDS)));
			        return future2;
				}
	        });
        }
        
        return future1;
    }

    /**
     * Retrieves dynamic information about a partition of an event hub (see {@link EventHubPartitionRuntimeInformation} for
     * details. Retries until it reaches the operation timeout, then either rethrows the last error if available or
     * returns null to indicate timeout.
     * 
     * @param partitionId  Partition to get information about. Must be one of the partition ids returned by getRuntimeInformation.
     * @return CompletableFuture which returns an EventHubPartitionRuntimeInformation on success, or null on timeout.  
     */
    @Override
    public CompletableFuture<EventHubPartitionRuntimeInformation> getPartitionRuntimeInformation(String partitionId) {
    	CompletableFuture<EventHubPartitionRuntimeInformation> future1 = null;
    	
    	throwIfClosed();

    	Map<String, String> request = new HashMap<String, String>();
        request.put(ClientConstants.MANAGEMENT_ENTITY_TYPE_KEY, ClientConstants.MANAGEMENT_PARTITION_ENTITY_TYPE);
        request.put(ClientConstants.MANAGEMENT_ENTITY_NAME_KEY, this.eventHubName);
        request.put(ClientConstants.MANAGEMENT_PARTITION_NAME_KEY, partitionId);
        request.put(ClientConstants.MANAGEMENT_OPERATION_KEY, ClientConstants.READ_OPERATION_VALUE);
        future1 = this.<EventHubPartitionRuntimeInformation>addManagementToken(request);

        if (future1 == null) {
	        future1 = managementWithRetry(request).thenComposeAsync(new Function<Map<String, Object>, CompletableFuture<EventHubPartitionRuntimeInformation>>() {
				@Override
				public CompletableFuture<EventHubPartitionRuntimeInformation> apply(Map<String, Object> rawdata) {
					CompletableFuture<EventHubPartitionRuntimeInformation> future2 = new CompletableFuture<EventHubPartitionRuntimeInformation>();
					future2.complete(new EventHubPartitionRuntimeInformation(
							(String)rawdata.get(ClientConstants.MANAGEMENT_ENTITY_NAME_KEY),
							(String)rawdata.get(ClientConstants.MANAGEMENT_PARTITION_NAME_KEY),
							(long)rawdata.get(ClientConstants.MANAGEMENT_RESULT_BEGIN_SEQUENCE_NUMBER),
							(long)rawdata.get(ClientConstants.MANAGEMENT_RESULT_LAST_ENQUEUED_SEQUENCE_NUMBER),
							(String)rawdata.get(ClientConstants.MANAGEMENT_RESULT_LAST_ENQUEUED_OFFSET),
							((Date)rawdata.get(ClientConstants.MANAGEMENT_RESULT_LAST_ENQUEUED_TIME_UTC)).toInstant()));
					return future2;
				}
	        });
        }
        
        return future1;
    }
    
    private <T> CompletableFuture<T> addManagementToken(Map<String, String> request)
    {
    	CompletableFuture<T> retval = null;
    	Exception failure = null;
        try {
        	String audience = String.format("amqp://%s/%s", this.underlyingFactory.getHostName(), this.eventHubName);
        	String token;
            token = this.underlyingFactory.getTokenProvider().getToken(audience, this.underlyingFactory.getOperationTimeout()).get().getToken();
            request.put(ClientConstants.MANAGEMENT_SECURITY_TOKEN_KEY, token);
		} 
        catch (InterruptedException | RuntimeException e) {
            retval = new CompletableFuture<T>();
            retval.completeExceptionally(e);
        } catch (ExecutionException e) {
            retval = new CompletableFuture<T>();
            final Throwable cause = e.getCause();

            //&& (cause instanceof InvalidKeyException || cause instanceof NoSuchAlgorithmException || cause instanceof IOException)
            retval.completeExceptionally(cause != null ? cause : e);
        }

        return retval;
    }
    
    private CompletableFuture<Map<String, Object>> managementWithRetry(Map<String, String> request) {
        Instant endTime = Instant.now().plus(this.underlyingFactory.getOperationTimeout());
        CompletableFuture<Map<String, Object>> rawdataFuture = new CompletableFuture<Map<String, Object>>();
        
        ManagementRetry retrier = new ManagementRetry(rawdataFuture, endTime, this.underlyingFactory, request);
        Timer.schedule(retrier, Duration.ZERO, TimerType.OneTimeRun);
        
        return rawdataFuture;
    }
    
    private class ManagementRetry implements Runnable {
    	private final CompletableFuture<Map<String, Object>> finalFuture;
    	private final Instant endTime;
    	private final MessagingFactory mf;
    	private final Map<String, String> request;
    	
    	public ManagementRetry(CompletableFuture<Map<String, Object>> future, Instant endTime, MessagingFactory mf,
    			Map<String, String> request) {
    		this.finalFuture = future;
    		this.endTime = endTime;
    		this.mf = mf;
    		this.request = request;
    	}
    	
		@Override
		public void run() {
			CompletableFuture<Map<String, Object>> intermediateFuture = this.mf.getManagementChannel().request(this.mf.getReactorScheduler(), request);
			intermediateFuture.whenComplete(new BiConsumer<Map<String, Object>, Throwable>() {
				@Override
				public void accept(Map<String, Object> result, Throwable error) {
					if ((result != null) && (error == null)) {
						// Success!
						ManagementRetry.this.finalFuture.complete(result);
					}
					else {
						Duration remainingTime = Duration.between(Instant.now(), ManagementRetry.this.endTime);
						Exception lastException = null;
						Throwable completeWith = error;
						if (error == null) {
							// Timeout, so fake up an exception to keep getNextRetryInternal happy.
							// It has to be a EventHubException that is set to retryable or getNextRetryInterval will halt the retries.
							lastException = new EventHubException(true, "timed out");
							completeWith = null;
						}
						else if (error instanceof Exception) {
							if ((error instanceof ExecutionException) && (error.getCause() != null) && (error.getCause() instanceof Exception)) {
							    if(error.getCause() instanceof AmqpException) {
							        lastException = ExceptionUtil.toException(((AmqpException) error.getCause()).getError());
                                }
                                else {
							        lastException = (Exception)error.getCause();
                                }

								completeWith = error.getCause();
							}
							else {
								lastException = (Exception)error;
							}
						}
						else {
							lastException = new Exception("got a throwable: " + error.toString());
						}
						Duration waitTime = ManagementRetry.this.mf.getRetryPolicy().getNextRetryInterval(ManagementRetry.this.mf.getClientId(), lastException, remainingTime);
						if (waitTime == null) {
							// Do not retry again, give up and report error.
							if (completeWith == null) {
								ManagementRetry.this.finalFuture.complete(null);
							}
							else {
								ManagementRetry.this.finalFuture.completeExceptionally(completeWith);
							}
						}
						else {
							// The only thing needed here is to schedule a new attempt. Even if the RequestResponseChannel has croaked,
							// ManagementChannel uses FaultTolerantObject, so the underlying RequestResponseChannel will be recreated
							// the next time it is needed.
							ManagementRetry retrier = new ManagementRetry(ManagementRetry.this.finalFuture, ManagementRetry.this.endTime,
									ManagementRetry.this.mf, ManagementRetry.this.request);
							Timer.schedule(retrier, waitTime, TimerType.OneTimeRun);
						}
					}
				}
			});
		}
    }
}
