/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


class Pump
{
    protected final EventProcessorHost host; // protected for testability

    protected ConcurrentHashMap<String, PartitionPump> pumpStates; // protected for testability

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(Pump.class);
    
    public Pump(EventProcessorHost host)
    {
        this.host = host;

        this.pumpStates = new ConcurrentHashMap<String, PartitionPump>();
    }
    
    public void addPump(Lease lease)
    {
    	PartitionPump capturedPump = this.pumpStates.get(lease.getPartitionId()); // CONCURRENTHASHTABLE
    	if (capturedPump == null)
    	{
    		// No existing pump, create a new one.
    		TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(Pump.this.host, lease, "creating new pump"));
    		PartitionPump newPartitionPump = createNewPump(lease);
    		this.pumpStates.put(lease.getPartitionId(), newPartitionPump);
    		
    		final String capturedPartitionId = lease.getPartitionId();
    		// These are fast, non-blocking actions, so it is OK to run on the same thread that was running pump shutdown.
    		// Also, do not run async because otherwise there can be a race with executor being shut down.
    		newPartitionPump.startPump().whenComplete((r,e) -> this.pumpStates.remove(capturedPartitionId))
    			.whenComplete((r,e) -> removingPumpTestHook(capturedPartitionId, e));
    	}
    	else
    	{
    		// There already is a pump. Shouldn't get here but do something sane if it happens -- just replace the lease.
			TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, lease, "updating lease for pump"));
			capturedPump.setLease(lease);
    	}
    }
    
    // Separated out so that tests can override and substitute their own pump class.
    protected PartitionPump createNewPump(Lease lease)
    {
    	return new PartitionPump(this.host, lease);
    }
    
    public CompletableFuture<Void> removePump(String partitionId, final CloseReason reason)
    {
    	CompletableFuture<Void> retval = CompletableFuture.completedFuture(null);
    	PartitionPump capturedPump = this.pumpStates.get(partitionId); // CONCURRENTHASHTABLE
    	if (capturedPump != null)
    	{
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, partitionId,
                    "closing pump for reason " + reason.toString()));
            retval = capturedPump.shutdown(reason);
    	}
    	else
    	{
    		// Shouldn't get here but not really harmful, so just trace.
    		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId,
                    "no pump found to remove for partition " + partitionId));
    	}
    	return retval;
    }

    public CompletableFuture<Void>[] removeAllPumps(CloseReason reason)
    {
    	// Java won't new an array of CompletableFuture<Void>.
    	CompletableFuture<Void>[] futures = (CompletableFuture<Void>[]) new Object[this.pumpStates.size()];
    	int i = 0;
    	for (String partitionId : this.pumpStates.keySet())
    	{
    		futures[i++] = removePump(partitionId, reason);
    	}
    	return futures;
    }
    
    protected void removingPumpTestHook(String partitionId, Throwable e)
    {
    	// For test use.
    }
}
