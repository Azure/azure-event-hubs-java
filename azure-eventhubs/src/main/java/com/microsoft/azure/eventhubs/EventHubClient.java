/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.io.IOException;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.microsoft.azure.eventhubs.impl.EventHubClientImpl;
import com.microsoft.azure.eventhubs.impl.ExceptionUtil;

/**
 * Anchor class - all EventHub client operations STARTS here.
 *
 * @see EventHubClient#createFromConnectionString(String, Executor)
 */
public interface EventHubClient {

    String DEFAULT_CONSUMER_GROUP_NAME = "$Default";

    /**
     * It will be truncated to 128 characters
     */
    String USER_AGENT = null;

    /**
     * Synchronous version of {@link #createFromConnectionString(String, Executor)}.
     *
     * @param connectionString The connection string to be used. See {@link ConnectionStringBuilder} to construct a connectionString.
     * @param executor An {@link Executor} to run all tasks performed by {@link EventHubClient}.
     * @return EventHubClient which can be used to create Senders and Receivers to EventHub
     * @throws EventHubException If Service Bus service encountered problems during connection creation.
     * @throws IOException         If the underlying Proton-J layer encounter network errors.
     */
    static EventHubClient createFromConnectionStringSync(final String connectionString, final Executor executor)
            throws EventHubException, IOException {
        return EventHubClient.createFromConnectionStringSync(connectionString, null, executor);
    }

    /**
     * Synchronous version of {@link #createFromConnectionString(String, Executor)}.
     *
     * @param connectionString The connection string to be used. See {@link ConnectionStringBuilder} to construct a connectionString.
     * @param retryPolicy      A custom {@link RetryPolicy} to be used when communicating with EventHub.
     * @param executor An {@link Executor} to run all tasks performed by {@link EventHubClient}.
     * @return EventHubClient which can be used to create Senders and Receivers to EventHub
     * @throws EventHubException If Service Bus service encountered problems during connection creation.
     * @throws IOException         If the underlying Proton-J layer encounter network errors.
     */
    static EventHubClient createFromConnectionStringSync(final String connectionString, final RetryPolicy retryPolicy, final Executor executor)
            throws EventHubException, IOException {
        return ExceptionUtil.syncWithIOException(() -> createFromConnectionString(connectionString, retryPolicy, executor).get());
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
    static CompletableFuture<EventHubClient> createFromConnectionString(final String connectionString, final Executor executor)
            throws EventHubException, IOException {
        return EventHubClient.createFromConnectionString(connectionString, null, executor);
    }

    /**
     * Factory method to create an instance of {@link EventHubClient} using the supplied connectionString.
     * In a normal scenario (when re-direct is not enabled) - one EventHubClient instance maps to one Connection to the Azure ServiceBus EventHubs service.
     * <p>The {@link EventHubClient} created from this method creates a Sender instance internally, which is used by the {@link #send(EventData)} methods.
     *
     * @param connectionString The connection string to be used. See {@link ConnectionStringBuilder} to construct a connectionString.
     * @param retryPolicy      A custom {@link RetryPolicy} to be used when communicating with EventHub.
     * @param executor An {@link Executor} to run all tasks performed by {@link EventHubClient}.
     * @return CompletableFuture{@literal <EventHubClient>} which can be used to create Senders and Receivers to EventHub
     * @throws EventHubException If Service Bus service encountered problems during connection creation.
     * @throws IOException         If the underlying Proton-J layer encounter network errors.
     */
    static CompletableFuture<EventHubClient> createFromConnectionString(
            final String connectionString, final RetryPolicy retryPolicy, final Executor executor)
            throws EventHubException, IOException {
        return EventHubClientImpl.createFromConnectionString(connectionString, retryPolicy, executor);
    }

    /**
     * Creates an Empty Collection of {@link EventData}.
     * The same partitionKey must be used while sending these events using {@link EventHubClient#send(EventDataBatch)}.
     *
     * @param options see {@link BatchOptions} for more details
     * @return the empty {@link EventDataBatch}, after negotiating maximum message size with EventHubs service
     * @throws EventHubException if the Microsoft Azure Event Hubs service encountered problems during the operation.
     */
    EventDataBatch createBatch(BatchOptions options) throws EventHubException;

    /**
     * Creates an Empty Collection of {@link EventData}.
     * The same partitionKey must be used while sending these events using {@link EventHubClient#send(EventDataBatch)}.
     *
     * @return the empty {@link EventDataBatch}, after negotiating maximum message size with EventHubs service
     * @throws EventHubException if the Microsoft Azure Event Hubs service encountered problems during the operation.
     */
    default EventDataBatch createBatch() throws EventHubException {
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
    default void sendSync(final EventData data) throws EventHubException {
        ExceptionUtil.syncVoid(() -> this.send(data).get());
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
    CompletableFuture<Void> send(final EventData data);

    /**
     * Synchronous version of {@link #send(Iterable)}.
     *
     * @param eventDatas batch of events to send to EventHub
     * @throws PayloadSizeExceededException if the total size of the {@link EventData} exceeds a pre-defined limit set by the service. Default is 256k bytes.
     * @throws EventHubException          if Service Bus service encountered problems during the operation.
     * @throws UnresolvedAddressException   if there are Client to Service network connectivity issues, if the Azure DNS resolution of the ServiceBus Namespace fails (ex: namespace deleted etc.)
     */
    default void sendSync(final Iterable<? extends EventData> eventDatas) throws EventHubException {
        ExceptionUtil.syncVoid(() -> this.send(eventDatas).get());
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
    CompletableFuture<Void> send(final Iterable<? extends EventData> eventDatas);

    /**
     * Synchronous version of {@link #send(EventDataBatch)}.
     *
     * @param eventDatas EventDataBatch to send to EventHub
     * @throws EventHubException        if Service Bus service encountered problems during the operation.
     */
    default void sendSync(final EventDataBatch eventDatas) throws EventHubException {
        ExceptionUtil.syncVoid(() -> this.send(eventDatas).get());
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
    CompletableFuture<Void> send(final EventDataBatch eventDatas);

    /**
     * Synchronous version of {@link #send(EventData, String)}.
     *
     * @param eventData    the {@link EventData} to be sent.
     * @param partitionKey the partitionKey will be hash'ed to determine the partitionId to send the eventData to. On the Received message this can be accessed at {@link EventData.SystemProperties#getPartitionKey()}
     * @throws PayloadSizeExceededException if the total size of the {@link EventData} exceeds a pre-defined limit set by the service. Default is 256k bytes.
     * @throws EventHubException          if Service Bus service encountered problems during the operation.
     */
    default void sendSync(final EventData eventData, final String partitionKey) throws EventHubException{
        ExceptionUtil.syncVoid(() -> this.send(eventData, partitionKey).get());
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
    CompletableFuture<Void> send(final EventData eventData, final String partitionKey);

    /**
     * Synchronous version of {@link #send(Iterable, String)}.
     *
     * @param eventDatas   the batch of events to send to EventHub
     * @param partitionKey the partitionKey will be hash'ed to determine the partitionId to send the eventData to. On the Received message this can be accessed at {@link EventData.SystemProperties#getPartitionKey()}
     * @throws PayloadSizeExceededException if the total size of the {@link EventData} exceeds a pre-defined limit set by the service. Default is 256k bytes.
     * @throws EventHubException          if Service Bus service encountered problems during the operation.
     * @throws UnresolvedAddressException   if there are Client to Service network connectivity issues, if the Azure DNS resolution of the ServiceBus Namespace fails (ex: namespace deleted etc.)
     */
    default void sendSync(final Iterable<? extends EventData> eventDatas, final String partitionKey) throws EventHubException{
        ExceptionUtil.syncVoid(() -> this.send(eventDatas, partitionKey).get());
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
    CompletableFuture<Void> send(final Iterable<? extends EventData> eventDatas, final String partitionKey);

    /**
     * Synchronous version of {@link #createPartitionSender(String)}.
     *
     * @param partitionId partitionId of EventHub to send the {@link EventData}'s to
     * @return PartitionSenderImpl which can be used to send events to a specific partition.
     * @throws EventHubException if Service Bus service encountered problems during connection creation.
     */
    default PartitionSender createPartitionSenderSync(final String partitionId) throws EventHubException, IllegalArgumentException{
        return ExceptionUtil.syncWithIllegalArgException(() -> this.createPartitionSender(partitionId).get());
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
     * @return a CompletableFuture that would result in a PartitionSenderImpl when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during connection creation.
     * @see PartitionSender
     */
    CompletableFuture<PartitionSender> createPartitionSender(final String partitionId) throws EventHubException;

    /**
     * Synchronous version of {@link #createReceiver(String, String, EventPosition)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param eventPosition     the position to start receiving the events from. See {@link EventPosition}
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    default PartitionReceiver createReceiverSync(final String consumerGroupName, final String partitionId, final EventPosition eventPosition) throws EventHubException{
        return ExceptionUtil.sync(() -> this.createReceiver(consumerGroupName, partitionId, eventPosition).get());
    }

    /**
     * Create the EventHub receiver with given partition id and start receiving from the specified starting offset.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param eventPosition     the position to start receiving the events from. See {@link EventPosition}
     * @return a CompletableFuture that would result in a PartitionReceiver instance when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     */
    CompletableFuture<PartitionReceiver> createReceiver(final String consumerGroupName, final String partitionId, final EventPosition eventPosition) throws EventHubException;

    /**
     * Synchronous version of {@link #createReceiver(String, String, EventPosition)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param eventPosition     the position to start receiving the events from. See {@link EventPosition}
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    default PartitionReceiver createReceiverSync(final String consumerGroupName, final String partitionId, final EventPosition eventPosition, final ReceiverOptions receiverOptions) throws EventHubException{
        return ExceptionUtil.sync(() -> this.createReceiver(consumerGroupName, partitionId, eventPosition, receiverOptions).get());
    }

    /**
     * Create the EventHub receiver with given partition id and start receiving from the specified starting offset.
     * The receiver is created for a specific EventHub Partition from the specific consumer group.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param eventPosition     the position to start receiving the events from. See {@link EventPosition}
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return a CompletableFuture that would result in a PartitionReceiver instance when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     */
    CompletableFuture<PartitionReceiver> createReceiver(final String consumerGroupName, final String partitionId, final EventPosition eventPosition, final ReceiverOptions receiverOptions) throws EventHubException;

    /**
     * Synchronous version of {@link #createEpochReceiver(String, String, EventPosition, long)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param eventPosition     the position to start receiving the events from. See {@link EventPosition}
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    default PartitionReceiver createEpochReceiverSync(final String consumerGroupName, final String partitionId, final EventPosition eventPosition, final long epoch) throws EventHubException{
        return ExceptionUtil.sync(() -> this.createEpochReceiver(consumerGroupName, partitionId, eventPosition, epoch).get());
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
     * @param eventPosition     the position to start receiving the events from. See {@link EventPosition}
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     * @see ReceiverDisconnectedException
     */
    CompletableFuture<PartitionReceiver> createEpochReceiver(final String consumerGroupName, final String partitionId, final EventPosition eventPosition, final long epoch) throws EventHubException;

    /**
     * Synchronous version of {@link #createEpochReceiver(String, String, EventPosition, long)}.
     *
     * @param consumerGroupName the consumer group name that this receiver should be grouped under.
     * @param partitionId       the partition Id that the receiver belongs to. All data received will be from this partition only.
     * @param eventPosition     the position to start receiving the events from. See {@link EventPosition}
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return PartitionReceiver instance which can be used for receiving {@link EventData}.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     */
    default PartitionReceiver createEpochReceiverSync(final String consumerGroupName, final String partitionId, final EventPosition eventPosition, final long epoch, final ReceiverOptions receiverOptions) throws EventHubException{
        return ExceptionUtil.sync(() -> this.createEpochReceiver(consumerGroupName, partitionId, eventPosition, epoch, receiverOptions).get());
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
     * @param eventPosition     the position to start receiving the events from. See {@link EventPosition}
     * @param epoch             an unique identifier (epoch value) that the service uses, to enforce partition/lease ownership.
     * @param receiverOptions   the set of options to enable on the event hubs receiver
     * @return a CompletableFuture that would result in a PartitionReceiver when it is completed.
     * @throws EventHubException if Service Bus service encountered problems during the operation.
     * @see PartitionReceiver
     * @see ReceiverDisconnectedException
     */
    CompletableFuture<PartitionReceiver> createEpochReceiver(final String consumerGroupName, final String partitionId, final EventPosition eventPosition, final long epoch, final ReceiverOptions receiverOptions) throws EventHubException;

    /**
     * Retrieves general information about an event hub (see {@link EventHubRuntimeInformation} for details).
     * Retries until it reaches the operation timeout, then either rethrows the last error if available or
     * returns null to indicate timeout.
     *
     * @return CompletableFuture which returns an EventHubRuntimeInformation on success, or null on timeout.
     */
    CompletableFuture<EventHubRuntimeInformation> getRuntimeInformation();

    /**
     * Retrieves dynamic information about a partition of an event hub (see {@link PartitionRuntimeInformation} for
     * details. Retries until it reaches the operation timeout, then either rethrows the last error if available or
     * returns null to indicate timeout.
     *
     * @param partitionId  Partition to get information about. Must be one of the partition ids returned by getReceiverRuntimeInformation.
     * @return CompletableFuture which returns an PartitionRuntimeInformation on success, or null on timeout.
     */
    CompletableFuture<PartitionRuntimeInformation> getPartitionRuntimeInformation(String partitionId);

    CompletableFuture<Void> close();

    void closeSync() throws EventHubException;
}
