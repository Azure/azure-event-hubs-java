/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.ReceiverRuntimeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionContext
{
    private final EventProcessorHost host;
    private final String partitionId;
    private final String eventHubPath;
    private final String consumerGroupName;
    
    private Lease lease;
    private String offset = PartitionReceiver.START_OF_STREAM;
    private long sequenceNumber = 0;
    private ReceiverRuntimeInformation runtimeInformation;

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(PartitionContext.class);
    
    PartitionContext(EventProcessorHost host, String partitionId, String eventHubPath, String consumerGroupName)
    {
        this.host = host;
        this.partitionId = partitionId;
        this.eventHubPath = eventHubPath;
        this.consumerGroupName = consumerGroupName;

        this.runtimeInformation = new ReceiverRuntimeInformation(partitionId);
    }

    public String getConsumerGroupName()
    {
        return this.consumerGroupName;
    }

    public String getEventHubPath()
    {
        return this.eventHubPath;
    }
    
    public String getOwner()
    {
    	return this.lease.getOwner();
    }
    
    public ReceiverRuntimeInformation getRuntimeInformation()
    {
        return this.runtimeInformation;
    }
    
    void setRuntimeInformation(ReceiverRuntimeInformation value)
    {
        this.runtimeInformation = value;
    }

    Lease getLease()
    {
        return this.lease;
    }

    // Unlike other properties which are immutable after creation, the lease is updated dynamically and needs a setter.
    void setLease(Lease lease)
    {
        this.lease = lease;
    }

    void setOffsetAndSequenceNumber(EventData event)
    {
		if (sequenceNumber >= this.sequenceNumber)
		{
			this.offset = event.getSystemProperties().getOffset();
			this.sequenceNumber = event.getSystemProperties().getSequenceNumber();
		}
		else
		{
			TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionId,
                    "setOffsetAndSequenceNumber(" + event.getSystemProperties().getOffset() + "//" +
					event.getSystemProperties().getSequenceNumber() + ") would move backwards, ignoring"));
		}
    }
    
    public String getPartitionId()
    {
    	return this.partitionId;
    }
    
    // Returns a String (offset) or Instant (timestamp).
    CompletableFuture<Object> getInitialOffset()
    {
    	Object startAt = null;
    	
    	TRACE_LOGGER.info(LoggingUtils.threadPoolStatusReport(this.host.getHostName(), this.host.getExecutorService()));
    	return this.host.getCheckpointManager().getCheckpoint(this.partitionId)
    	.thenApply((startingCheckpoint) ->
    	{
    		return checkpointToOffset(startingCheckpoint);
    	});
    }
    
    Object checkpointToOffset(Checkpoint startingCheckpoint)
    {
    	Object startAt = null;
    	if (startingCheckpoint == null)
    	{
    		// No checkpoint was ever stored. Use the initialOffsetProvider instead.
        	Function<String, Object> initialOffsetProvider = this.host.getEventProcessorOptions().getInitialOffsetProvider();
    		TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionId,
                    "Calling user-provided initial offset provider"));
    		startAt = initialOffsetProvider.apply(this.partitionId);
    		if (startAt instanceof String)
    		{
    			this.offset = (String)startAt;
        		this.sequenceNumber = 0; // TODO we use sequenceNumber to check for regression of offset, 0 could be a problem until it gets updated from an event
    	    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionId,
                        "Initial offset provided: " + this.offset + "//" + this.sequenceNumber));
    		}
    		else if (startAt instanceof Instant)
    		{
    			// can't set offset/sequenceNumber
    	    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionId,
                        "Initial timestamp provided: " + (Instant)startAt));
    		}
    		else
    		{
    			throw new IllegalArgumentException("Unexpected object type returned by user-provided initialOffsetProvider");
    		}
    	}
    	else
    	{
    		// Checkpoint is valid, use it.
	    	this.offset = startingCheckpoint.getOffset();
	    	startAt = this.offset;
	    	this.sequenceNumber = startingCheckpoint.getSequenceNumber();
	    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionId,
                    "Retrieved starting offset " + this.offset + "//" + this.sequenceNumber));
    	}
    	
    	return startAt;
    }

    /**
     * Writes the current offset and sequenceNumber to the checkpoint store via the checkpoint manager.
     * It is important to check the result in order to detect failures.
     * 
     * @return A CompletableFuture that completes when the checkpoint is updated (result is null) or the update fails (exceptional completion).
     */
    public CompletableFuture<Void> checkpoint()
    {
    	Checkpoint capturedCheckpoint = new Checkpoint(this.partitionId, this.offset, this.sequenceNumber);
    	return persistCheckpoint(capturedCheckpoint);
    }

    /**
     * Stores the offset and sequenceNumber from the provided received EventData instance, then writes those
     * values to the checkpoint store via the checkpoint manager.
     * It is important to check the result in order to detect failures.
     *  
     * @param event  A received EventData with valid offset and sequenceNumber
     * @return A CompletableFuture that completes when the checkpoint is updated (result is null) or the update fails (exceptional completion).
     */
    public CompletableFuture<Void> checkpoint(EventData event)
    {
    	return persistCheckpoint(new Checkpoint(this.partitionId, event.getSystemProperties().getOffset(), event.getSystemProperties().getSequenceNumber()));
    }
    
    private CompletableFuture<Void> persistCheckpoint(Checkpoint persistThis)
    {
    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, persistThis.getPartitionId(),
                "Saving checkpoint: " + persistThis.getOffset() + "//" + persistThis.getSequenceNumber()));
		
        return this.host.getCheckpointManager().updateCheckpoint(this.lease, persistThis);
    }
}
