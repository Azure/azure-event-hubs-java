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

    private ConcurrentHashMap<String, PartitionPump> pumpStates;

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(Pump.class);
    
    public Pump(EventProcessorHost host)
    {
        this.host = host;

        this.pumpStates = new ConcurrentHashMap<String, PartitionPump>();
    }
    
    public void addPump(Lease lease)
    {
    	PartitionPump capturedPump = this.pumpStates.get(lease.getPartitionId());
    	if (capturedPump == null)
    	{
    		// No existing pump, create a new one.
    		createNewPump(lease.getPartitionId(), lease);
    	}
    	else
    	{
    		// There already is a pump. Shouldn't get here but do something sane if it happens -- just replace the lease.
			TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), lease.getPartitionId(), "updating lease for pump"));
			capturedPump.setLease(lease);
    	}
    }
    
    private void createNewPump(String partitionId, Lease lease)
    {
		TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(Pump.this.host.getHostName(), partitionId, "creating new pump"));
		PartitionPump newPartitionPump = new PartitionPump(this.host, lease);
		this.pumpStates.put(partitionId, newPartitionPump);
		
		final String capturedPartitionId = partitionId;
		newPartitionPump.startPump().whenCompleteAsync((r,e) -> this.pumpStates.remove(capturedPartitionId), this.host.getExecutorService());
    }
    
    public CompletableFuture<Void> removePump(String partitionId, final CloseReason reason)
    {
    	CompletableFuture<Void> retval = CompletableFuture.completedFuture(null);
    	PartitionPump capturedPump = this.pumpStates.get(partitionId);
    	if (capturedPump != null)
    	{
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), partitionId,
                    "closing pump for reason " + reason.toString()));
            retval = capturedPump.shutdown(reason);
    	}
    	else
    	{
    		// Shouldn't get here but not really harmful, so just trace.
    		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), partitionId,
                    "no pump found to remove for partition " + partitionId));
    	}
    	return retval;
    }

    public Iterable<CompletableFuture<Void>> removeAllPumps(CloseReason reason)
    {
    	ArrayList<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
    	for (String partitionId : this.pumpStates.keySet())
    	{
    		futures.add(removePump(partitionId, reason));
    	}
    	return futures;
    }
}
