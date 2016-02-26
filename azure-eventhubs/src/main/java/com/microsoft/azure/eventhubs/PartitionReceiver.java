/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import org.apache.qpid.proton.message.Message;
import com.microsoft.azure.servicebus.*;

/**
 * This is a logical representation of receiving from a EventHub partition.
 * <p>
 * A PartitionReceiver is tied to a ConsumerGroup + Partition combination. If you are creating an epoch based 
 * PartitionReceiver (i.e. PartitionReceiver.getEpoch != 0) you cannot have more than one active receiver per 
 * ConsumerGroup + Partition combo. You can have multiple receivers per ConsumerGroup + Partition combo with 
 * non-epoch receivers.
 *
 * @see {@link EventHubClient#createReceiver}
 * @see {@link EventHubClient#createEpochReceiver} 
 */
public final class PartitionReceiver extends ClientEntity
{
	private static final int MINIMUM_PREFETCH_COUNT = 10;
	private static final int MAXIMUM_PREFETCH_COUNT = 999;
	
	static final int DEFAULT_PREFETCH_COUNT = 300;
	static final long NULL_EPOCH = 0;
	
    /**
     * This is a constant defined to represent the start of a partition stream in EventHub.
     */
	public static final String START_OF_STREAM = "-1";
	
	private final String partitionId;
	private final MessagingFactory underlyingFactory;
	private final String eventHubName;
	private final String consumerGroupName;
	
	private String startingOffset;
	private boolean offsetInclusive;
	private Instant startingDateTime;
	private MessageReceiver internalReceiver; 
	private Long epoch;
	private boolean isEpochReceiver;
	
	private PartitionReceiver(MessagingFactory factory, 
			final String eventHubName, 
			final String consumerGroupName, 
			final String partitionId, 
			final String startingOffset, 
			final boolean offsetInclusive,
			final Instant dateTime,
			final Long epoch,
			final boolean isEpochReceiver)
					throws ServiceBusException
	{
		super(null);
		this.underlyingFactory = factory;
		this.eventHubName = eventHubName;
		this.consumerGroupName = consumerGroupName;
		this.partitionId = partitionId;
		this.startingOffset = startingOffset;
		this.offsetInclusive = offsetInclusive;
		this.startingDateTime = dateTime;
		this.epoch = epoch;
		this.isEpochReceiver = isEpochReceiver;
	}
	
	static CompletableFuture<PartitionReceiver> create(MessagingFactory factory, 
			final String eventHubName, 
			final String consumerGroupName, 
			final String partitionId, 
			final String startingOffset, 
			final boolean offsetInclusive,
			final Instant dateTime,
			final long epoch,
			final boolean isEpochReceiver) 
					throws ServiceBusException
	{
        if (epoch < NULL_EPOCH)
        {
            throw new IllegalArgumentException("epoch cannot be a negative value. Please specify a zero or positive long value.");
        }
        
		if (StringUtil.isNullOrWhiteSpace(consumerGroupName))
		{
			throw new IllegalArgumentException("specify valid string for argument - 'consumerGroupName'");
		}
			
		final PartitionReceiver receiver = new PartitionReceiver(factory, eventHubName, consumerGroupName, partitionId, startingOffset, offsetInclusive, dateTime, epoch, isEpochReceiver);
		return receiver.createInternalReceiver().thenApplyAsync(new Function<Void, PartitionReceiver>()
		{
			public PartitionReceiver apply(Void a)
			{
				return receiver;
			}
		});
	}
	
	private CompletableFuture<Void> createInternalReceiver() throws ServiceBusException
	{
		return MessageReceiver.create(this.underlyingFactory, StringUtil.getRandomString(), 
				String.format("%s/ConsumerGroups/%s/Partitions/%s", this.eventHubName, this.consumerGroupName, this.partitionId), 
				this.startingOffset, this.offsetInclusive, this.startingDateTime, PartitionReceiver.DEFAULT_PREFETCH_COUNT, this.epoch, this.isEpochReceiver)
				.thenAcceptAsync(new Consumer<MessageReceiver>()
				{
					public void accept(MessageReceiver r) { PartitionReceiver.this.internalReceiver = r;}
				});
	}
	
	/**
	 * @return The Cursor from which this Receiver started receiving from
	 */
	final String getStartingOffset()
	{
		return this.startingOffset;
	}
	
	final boolean getOffsetInclusive()
	{
		return this.offsetInclusive;
	}
	
	/**
	 * @return The identifier representing the partition from which this receiver is fetching data
	 */
	public final String getPartitionId()
	{
		return this.partitionId;
	}
	
    /**
     * @return the upper limit of events this receiver will actively receive regardless of whether a receive operation is pending.
     * @see {@link #setPrefetchCount}
     */
	public final int getPrefetchCount()
	{
		return this.internalReceiver.getPrefetchCount();
	}
	
	/**
	 * Set the number of events that can be pre-fetched and cached at the {@link PartitionReceiver).
	 * <p>
     * by default the value is 300
	 * @param prefetchCount the number of events to pre-fetch. value must be between 10 and 999. Default is 300.
	 */
	public final void setPrefetchCount(final int prefetchCount)
	{
		if (prefetchCount < PartitionReceiver.MINIMUM_PREFETCH_COUNT && prefetchCount > PartitionReceiver.MAXIMUM_PREFETCH_COUNT)
		{
			throw new IllegalArgumentException(String.format(Locale.US, 
					"PrefetchCount has to be between %s and %s", PartitionReceiver.MINIMUM_PREFETCH_COUNT, PartitionReceiver.MAXIMUM_PREFETCH_COUNT));
		}
		
		this.internalReceiver.setPrefetchCount(prefetchCount);
	}
	
    /**
     * Get the epoch value that this receiver is currently using for partition ownership.
     * <p>
     * A value of 0 means this receiver is not an epoch-based receiver.
     */
	public final long getEpoch()
	{
		return this.epoch;
	}
	
    /**
	 * Synchronous version of {@link #receive}. 
	 */
    public final Iterable<EventData> receiveSync() 
			throws ServiceBusException
	{
        try
        {
            return this.receive().get();
        }
		catch (InterruptedException|ExecutionException exception)
		{
            if (exception instanceof InterruptedException)
            {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }
            
			Throwable throwable = exception.getCause();
			if (throwable != null)
			{
				if (throwable instanceof RuntimeException)
				{
					throw (RuntimeException)throwable;
				}
				
				if (throwable instanceof ServiceBusException)
				{
					throw (ServiceBusException)throwable;
				}
				                
				throw new ServiceBusException(true, throwable);
			}
		}
        
		return null;
    }
	
	/** 
	 * Receive a batch of {@link EventData}'s from an EventHub partition
     * <p>
     * Sample code (sample uses sync version of the api but concept are identical):
     * <pre>{@code 
     * EventHubClient client = EventHubClient.createFromConnectionStringSync("__connection__");
     * PartitionReceiver receiver = client.createPartitionReceiverSync("ConsumerGroup1", "1");
	 * Iterable<EventData> receivedEvents = receiver.receiveSync();
	 *      
	 * while (true)
	 * {
	 *     int batchSize = 0;
	 *     if (receivedEvents != null)
	 *     {
	 *         for(EventData receivedEvent: receivedEvents)
	 *         {
	 *             System.out.println(String.format("Message Payload: %s", new String(receivedEvent.getBody(), Charset.defaultCharset())));
	 *             System.out.println(String.format("Offset: %s, SeqNo: %s, EnqueueTime: %s", 
	 *                 receivedEvent.getSystemProperties().getOffset(), 
	 *                 receivedEvent.getSystemProperties().getSequenceNumber(), 
	 *                 receivedEvent.getSystemProperties().getEnqueuedTime()));
	 *             batchSize++;
	 *         }
	 *     }
	 *          
	 *     System.out.println(String.format("ReceivedBatch Size: %s", batchSize));
	 *     receivedEvents = receiver.receiveSync();
	 * }
     * }</pre>
	 * @return A completableFuture that will yield a batch of {@link EventData}'s from the partition on which this receiver is created. Returns 'null' if no {@link EventData} is present.
	 * @throws ServerBusyException
	 * @throws AuthorizationFailedException
	 * @throws InternalServerException
	 */
	public CompletableFuture<Iterable<EventData>> receive()
			throws ServiceBusException
	{
		return this.internalReceiver.receive().thenApply(new Function<Collection<Message>, Iterable<EventData>>()
		{
			@Override
			public Iterable<EventData> apply(Collection<Message> amqpMessages)
			{
				return EventDataUtil.toEventDataCollection(amqpMessages);
			}			
		});
	}

	void setReceiveHandler(PartitionReceiveHandler receiveHandler)
	{
		this.internalReceiver.setReceiveHandler(receiveHandler);
	}

	public CompletableFuture<Void> close()
	{
		if (this.internalReceiver != null)
		{
			return this.internalReceiver.close();
		}
		else
		{
			return CompletableFuture.completedFuture(null);
		}
	}
}