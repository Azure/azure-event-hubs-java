/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.ReceiverDisconnectedException;
import com.microsoft.azure.eventhubs.ReceiverOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionPump extends PartitionReceiveHandler
{
	protected final EventProcessorHost host; // protected for testability
	protected Lease lease = null; // protected for testability

	private EventHubClient eventHubClient = null;
	private PartitionReceiver partitionReceiver = null;

	private CompletableFuture<Void> shutdownTriggerFuture = null;
	private CompletableFuture<Void> shutdownFinishedFuture = null;
	private CloseReason shutdownReason;

    private CompletableFuture<?> internalOperationFuture = null;
	
    private IEventProcessor processor = null;
    private PartitionContext partitionContext = null;
    
    private final Object processingSynchronizer;
    
    private ScheduledFuture<?> leaseRenewerFuture = null;

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(PartitionPump.class);
    
	PartitionPump(EventProcessorHost host, Lease lease)
	{
		super(host.getEventProcessorOptions().getMaxBatchSize());
		
		this.host = host;
		this.lease = lease;
		this.processingSynchronizer = new Object();
	}
	
	void setLease(Lease newLease)
	{
		this.lease = newLease;
		if (this.partitionContext != null)
		{
			this.partitionContext.setLease(newLease);
		}
	}
	
	// The CompletableFuture returned by startPump remains uncompleted as long as the pump is running.
	// If startup fails, or an error occurs while running, it will complete exceptionally.
	// If clean shutdown due to unregister call, it completes normally.
    CompletableFuture<Void> startPump()
    {
    	// Fast, non-blocking actions.
    	setupPartitionContext();
        
        // Set up the shutdown future. The shutdown process can be triggered just by completing this.shutdownFuture.
        this.shutdownTriggerFuture = new CompletableFuture<Void>();
        this.shutdownFinishedFuture = this.shutdownTriggerFuture.whenCompleteAsync((r,e) -> cancelPendingOperations(), this.host.getExecutorService())
        .thenComposeAsync((empty) -> cleanUpAll(this.shutdownReason), this.host.getExecutorService())
        .thenComposeAsync((empty) -> releaseLeaseOnShutdown(), this.host.getExecutorService());
        
        // Do the slow startup stuff asynchronously.
        // Use thenRun so that startup stages only execute if previous stages succeeded.
        // Use whenComplete to trigger cleanup on exception.
        CompletableFuture.runAsync(() -> openProcessor(), this.host.getExecutorService())
        .thenComposeAsync((empty) -> openClientsRetryWrapper(), this.host.getExecutorService())
        .thenRunAsync(() -> scheduleLeaseRenewer(), this.host.getExecutorService())
        .whenCompleteAsync((r,e) ->
        {
    		if (e != null)
    		{
    			// If startup failed, trigger shutdown to clean up.
    			internalShutdown(CloseReason.Shutdown, e);
    		}
    	}, this.host.getExecutorService());
        
        return shutdownFinishedFuture;
    }
    
    protected void setupPartitionContext()
    {
        this.partitionContext = new PartitionContext(this.host, this.lease.getPartitionId(), this.host.getEventHubPath(), this.host.getConsumerGroupName());
        this.partitionContext.setLease(this.lease);
    }
    
    private void openProcessor()
    {
        TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "Creating and opening event processor instance"));

    	String action = EventProcessorHostActionStrings.CREATING_EVENT_PROCESSOR;
    	try
    	{
			this.processor = this.host.getProcessorFactory().createEventProcessor(this.partitionContext);
			action = EventProcessorHostActionStrings.OPENING_EVENT_PROCESSOR;
            this.processor.onOpen(this.partitionContext);
    	}
        catch (Exception e)
        {
        	// If the processor won't create or open, only thing we can do here is pass the buck.
        	// Null it out so we don't try to operate on it further.
        	this.processor = null;
        	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "Failed " + action), e);
        	this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), e, action, this.lease.getPartitionId());
        	throw new CompletionException(e);
        }
    }
    
    private CompletableFuture<Void> openClientsRetryWrapper()
    {
    	// Stage 0: first attempt
    	CompletableFuture<Boolean> retryResult = openClients();
    	
    	for (int i = 1; i < 5; i++)
    	{
    		retryResult = retryResult
    		// Stages 1, 3, 5, etc: trace errors but stop exception propagation in order to keep going
    		// UNLESS it's ReceiverDisconnectedException.
    		.handleAsync((r,e) ->
    		{
    			if (e != null)
    			{
    				Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, null);
    				if (notifyWith instanceof ReceiverDisconnectedException)
    				{
    	        		// TODO Assuming this is due to a receiver with a higher epoch.
    	        		// Is there a way to be sure without checking the exception text?
    					// DO NOT trace here because then we could get multiple traces for the same exception.
    	        		// If it's a bad epoch, then retrying isn't going to help.
                        // Rethrow to keep propagating error to the end and prevent any more attempts.
                        throw new CompletionException(notifyWith);
    				}
    				else
    				{
    					TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                                "Failure creating client or receiver, retrying"), e);
    				}
    			}
    			// If we have a valid result, pass it along to prevent further attempts.
    			return (e == null) ? r : false;
    		}, this.host.getExecutorService())
    		// Stages 2, 4, 6, etc: make another attempt if needed.
    		.thenComposeAsync((done) -> 
    		{
    			return done ? CompletableFuture.completedFuture(done) : openClients();
    		}, this.host.getExecutorService());
    	}
    	// Stage final: on success, hook up the user's message handler to start receiving messages. On error,
    	// trace exceptions from the final attempt, or ReceiverDisconnectedException.
    	return retryResult.handleAsync((r,e) ->
    	{
    		if (e == null)
    		{
                // IEventProcessor.onOpen is called from the base PartitionPump and must have returned in order for execution to reach here, 
                // meaning it is safe to set the handler and start calling IEventProcessor.onEvents.
                this.partitionReceiver.setReceiveHandler(this, this.host.getEventProcessorOptions().getInvokeProcessorAfterReceiveTimeout());
    		}
    		else
    		{
				Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, null);
				if (notifyWith instanceof ReceiverDisconnectedException)
				{
	        		// TODO Assuming this is due to a receiver with a higher epoch.
	        		// Is there a way to be sure without checking the exception text?
                    TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                            "Receiver disconnected on create, bad epoch?"), notifyWith);
				}
				else
				{
					TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                            "Failure creating client or receiver, out of retries"), e);
				}
				
	            // IEventProcessor.onOpen is called from the base PartitionPump and must have returned in order for execution to reach here, 
	    		// so we can report this error to it instead of the general error handler.
	    		this.processor.onError(this.partitionContext, new ExceptionWithAction(notifyWith, EventProcessorHostActionStrings.CREATING_EVENT_HUB_CLIENT));
				
				// Rethrow so caller will see failure
				throw LoggingUtils.wrapException(notifyWith, EventProcessorHostActionStrings.CREATING_EVENT_HUB_CLIENT);
    		}
    		return null;
    	}, this.host.getExecutorService());
    }
    
    protected void scheduleLeaseRenewer()
    {
    	int seconds = this.host.getPartitionManagerOptions().getLeaseRenewIntervalInSeconds(); 
		this.leaseRenewerFuture = this.host.getExecutorService().schedule(() -> leaseRenewer(), seconds, TimeUnit.SECONDS);
    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.lease,
    			"scheduling leaseRenewer in " + seconds));
    }

    private CompletableFuture<Boolean> openClients()
    {
    	// Create new client
        TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "Opening EH client"));
        
        CompletableFuture<EventHubClient> startOpeningFuture = null;
        try
        {
			startOpeningFuture = EventHubClient.createFromConnectionString(this.host.getEventHubConnectionString());
		}
        catch (EventHubException | IOException e2)
        {
        	// Marking startOpeningFuture as completed exceptionally will cause all the
        	// following stages to fall through except stage 1 which will report the error.
        	startOpeningFuture = new CompletableFuture<EventHubClient>();
        	startOpeningFuture.completeExceptionally(e2);
        } 
		this.internalOperationFuture = startOpeningFuture;
		
		// Stage 0: get EventHubClient
		return startOpeningFuture
		// Stage 1: save EventHubClient on success, trace on error
		.whenCompleteAsync((ehclient, e) ->
		{
			if ((ehclient != null) && (e == null)) 
			{
				this.eventHubClient = ehclient;
			}
			else
			{
				TRACE_LOGGER.error(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "EventHubClient creation failed"), e);
			}
			// this.internalOperationFuture allows canceling startup if it gets stuck. Null out now that EventHubClient creation has completed.
			this.internalOperationFuture = null;
		}, this.host.getExecutorService())
		// Stage 2: get initial offset for receiver
		.thenComposeAsync((empty) -> this.partitionContext.getInitialOffset(), this.host.getExecutorService())
		// Stage 3: set up other receiver options, create receiver if initial offset is valid
		.thenComposeAsync((startAt) ->
		{
	        ReceiverOptions options = new ReceiverOptions();
	        options.setReceiverRuntimeMetricEnabled(this.host.getEventProcessorOptions().getReceiverRuntimeMetricEnabled());
	    	long epoch = this.lease.getEpoch();
	    	
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                    "Opening EH receiver with epoch " + epoch + " at location " + startAt));
	    	
	    	CompletableFuture<PartitionReceiver> receiverFuture = null;
	    	
	    	try
	    	{
	        	if (startAt instanceof String)
	        	{
					receiverFuture = this.eventHubClient.createEpochReceiver(this.partitionContext.getConsumerGroupName(),
							this.partitionContext.getPartitionId(), (String)startAt, epoch, options);
	        	}
	        	else if (startAt instanceof Instant) 
	        	{
	        		receiverFuture = this.eventHubClient.createEpochReceiver(this.partitionContext.getConsumerGroupName(),
	        				this.partitionContext.getPartitionId(), (Instant)startAt, epoch, options);
	        	}
	        	else
	        	{
	        		String errMsg = "Starting offset is not String or Instant, is " + ((startAt != null) ? startAt.getClass().toString() : "null");
	                TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, errMsg));
	                receiverFuture = new CompletableFuture<PartitionReceiver>();
	                receiverFuture.completeExceptionally(new RuntimeException(errMsg));
	        	}
	    	}
	    	catch (EventHubException e)
	    	{
                receiverFuture = new CompletableFuture<PartitionReceiver>();
                receiverFuture.completeExceptionally(e);
	    	}

        	return receiverFuture;
		}, this.host.getExecutorService())
		// Stage 4: save PartitionReceiver on success, trace on error
		.whenCompleteAsync((receiver, e) ->
		{
			if ((receiver != null) && (e == null))
			{
				this.partitionReceiver = receiver;
			}
			else if (this.eventHubClient != null)
			{
				TRACE_LOGGER.error(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "PartitionReceiver creation failed"), e);
			}
			// else if this.eventHubClient is null then we failed in stage 0 and already traced in stage 1
			
			// this.internalOperationFuture allows canceling startup if it gets stuck. Null out now that PartitionReceiver creation has completed.
			this.internalOperationFuture = null;
		}, this.host.getExecutorService())
		// Stage 5: on success, set up the receiver
		.thenApplyAsync((receiver) ->
		{
			try
			{
				this.partitionReceiver.setPrefetchCount(this.host.getEventProcessorOptions().getPrefetchCount());
			}
			catch (Exception e1)
			{
				TRACE_LOGGER.error(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "PartitionReceiver failed setting prefetch count"), e1);
				throw new CompletionException(e1);
			}
			this.partitionReceiver.setReceiveTimeout(this.host.getEventProcessorOptions().getReceiveTimeOut());

	        TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
	                "EH client and receiver creation finished"));
	        
	        return true;
		}, this.host.getExecutorService());
    }
    
    private CompletableFuture<Void> cleanUpAll(CloseReason reason) // swallows all exceptions
    {
    	return cleanUpClients()
    	.thenRunAsync(() ->
    	{
            if (this.processor != null)
            {
                try
                {
                	synchronized(this.processingSynchronizer)
                	{
                		// When we take the lock, any existing onEvents call has finished.
                    	// Because the client has been closed, there will not be any more
                    	// calls to onEvents in the future. Therefore we can safely call onClose.
                		this.processor.onClose(this.partitionContext, reason);
                    }
                }
                catch (Exception e)
                {
                	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                            "Failure closing processor"), e);
                	// If closing the processor has failed, the state of the processor is suspect.
                	// Report the failure to the general error handler instead.
                	this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), e, EventProcessorHostActionStrings.CLOSING_EVENT_PROCESSOR,
                			this.lease.getPartitionId());
                }
            }
    	}, this.host.getExecutorService());
    }
    
    private CompletableFuture<Void> cleanUpClients() // swallows all exceptions
    {
    	CompletableFuture<Void> cleanupFuture = null;
    	if (this.partitionReceiver != null)
    	{
			// Disconnect the processor from the receiver we're about to close.
			// Fortunately this is idempotent -- setting the handler to null when it's already been
			// nulled by code elsewhere is harmless!
			// Setting to null also waits for the in-progress calls to complete
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                    "Setting receive handler to null"));
    		cleanupFuture = this.partitionReceiver.setReceiveHandler(null);
    	}
    	else
    	{
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                    "partitionReceiver is null in cleanup"));
    		cleanupFuture = CompletableFuture.completedFuture(null);
    	}
    	cleanupFuture = cleanupFuture.handleAsync((empty, e) ->
    	{
    		if (e != null)
    		{
				TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
						"Got exception when ReceiveHandler is set to null."), LoggingUtils.unwrapException(e, null));
    		}
    		return null; // stop propagation of exceptions
    	}, this.host.getExecutorService())
    	.thenApplyAsync((empty) ->
    	{
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "Closing EH receiver"));
    		PartitionReceiver partitionReceiverTemp = this.partitionReceiver;
    		this.partitionContext = null;
    		return partitionReceiverTemp;
    	}, this.host.getExecutorService())
    	.thenComposeAsync((partitionReceiverTemp) -> 
    	{
    		return (partitionReceiverTemp != null) ? partitionReceiverTemp.close() : CompletableFuture.completedFuture(null);
    	}, this.host.getExecutorService())
    	.handleAsync((empty, e) ->
    	{
    		if (e != null)
    		{
                TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                        "Closing EH receiver failed."), LoggingUtils.unwrapException(e, null));
    		}
    		return null; // stop propagation of exceptions
    	}, this.host.getExecutorService())
    	.thenApplyAsync((empty) ->
    	{
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "Closing EH client"));
			final EventHubClient eventHubClientTemp = this.eventHubClient;
			this.eventHubClient = null;
			if (eventHubClientTemp == null)
			{
	            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
	                    "eventHubClient is null in cleanup"));
			}
			return eventHubClientTemp;
    	}, this.host.getExecutorService())
    	.thenComposeAsync((eventHubClientTemp) ->
    	{
    		return (eventHubClientTemp != null) ? eventHubClientTemp.close() : CompletableFuture.completedFuture(null);
    	}, this.host.getExecutorService())
    	.handleAsync((empty, e) ->
    	{
    		if (e != null)
    		{
				TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "Closing EH client failed."),
						LoggingUtils.unwrapException(e, null));
			}
            return null; // stop propagation of exceptions
    	}, this.host.getExecutorService());
    	
    	return cleanupFuture;
    }
    
    protected void cancelPendingOperations()
    {
    	// If an open operation is stuck, this lets us shut down anyway.
    	CompletableFuture<?> captured = this.internalOperationFuture;
    	if (captured != null)
    	{
    		captured.cancel(true);
    	}

    	ScheduledFuture<?> capturedLeaseRenewer = this.leaseRenewerFuture;
    	if (capturedLeaseRenewer != null)
    	{
    		this.leaseRenewerFuture.cancel(true);
    	}

    }
    
    private CompletableFuture<Void> releaseLeaseOnShutdown() // swallows all exceptions
    {
    	CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
    	
        if (this.shutdownReason != CloseReason.LeaseLost)
        {
	        // Since this pump is dead, release the lease. Don't care about any errors that may occur. Worst case is
        	// that the lease eventually expires, since the lease renewer has been cancelled.
        	result = PartitionPump.this.host.getLeaseManager().releaseLease(this.partitionContext.getLease())
        	.handleAsync((success, e) ->
        	{
        		if (e != null)
    	        {
    	        	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
    	        			"Failure releasing lease on pump shutdown"), LoggingUtils.unwrapException(e, null));
    			}
        		else if (!success)
        		{
        			TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext, "releaseLease() returned false on pump shutdown"));
        		}
        		return null; // stop propagation of exceptions
        	}, this.host.getExecutorService());
        }
        // else we already lost the lease, releasing is unnecessary and would fail if we try
        
        return result;
    }
    
    protected void internalShutdown(CloseReason reason, Throwable e)
    {
    	this.shutdownReason = reason;
    	if (e == null)
    	{
    		this.shutdownTriggerFuture.complete(null);
    	}
    	else
    	{
    		this.shutdownTriggerFuture.completeExceptionally(e);
    	}
    }

    CompletableFuture<Void> shutdown(CloseReason reason)
    {
        TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                "pump shutdown for reason " + reason.toString()));
    	internalShutdown(reason, null);
    	return this.shutdownFinishedFuture;
    }
    
    private void leaseRenewer()
    {
    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.lease, "leaseRenewer()"));
    	
    	// Theoretically, if the future is cancelled then this method should never fire, but
    	// there's no harm in being sure.
    	if (this.leaseRenewerFuture.isCancelled())
    	{
    		return;
    	}

    	// Stage 0: renew the lease
    	this.host.getLeaseManager().renewLease(this.lease)
    	// Stage 1: check result of renewing
    	.thenApplyAsync((renewed) ->
    	{
        	Boolean scheduleNext = true;
    		if (!renewed)
    		{
				// False return from renewLease means that lease was lost.
				// Start pump shutdown process and do not schedule another call to leaseRenewer.
	    		TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, this.lease, "Lease lost, shutting down pump"));
				internalShutdown(CloseReason.LeaseLost, null);
				scheduleNext = false;
    		}
    		return scheduleNext;
    	}, this.host.getExecutorService())
    	// Stage 2: RUN REGARDLESS OF EXCEPTIONS -- trace exceptions, schedule next iteration
    	.whenCompleteAsync((scheduleNext, e) ->
    	{
    		if (e != null)
    		{
        		// Failure renewing lease due to storage exception or whatever.
        		// Trace error and leave scheduleNext as true to schedule another try.
        		Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, null);
        		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, this.lease, "Transient failure renewing lease"), notifyWith);
        		// Notify the general error handler rather than calling this.processor.onError so we can provide context (RENEWING_LEASE)
        		this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith, EventProcessorHostActionStrings.RENEWING_LEASE,
        				this.lease.getPartitionId());
    		}
    		
    		if ((scheduleNext != null) && scheduleNext && !this.leaseRenewerFuture.isCancelled())
			{
				scheduleLeaseRenewer();
			}
    	}, this.host.getExecutorService());
    }

	@Override
	public void onReceive(Iterable<EventData> events)
	{
        if (this.host.getEventProcessorOptions().getReceiverRuntimeMetricEnabled())
        {
            this.partitionContext.setRuntimeInformation(this.partitionReceiver.getRuntimeInformation());
        }
        
        // This method is called on the thread that the Java EH client uses to run the pump.
        // There is one pump per EventHubClient. Since each PartitionPump creates a new EventHubClient,
        // using that thread to call onEvents does no harm. Even if onEvents is slow, the pump will
        // get control back each time onEvents returns, and be able to receive a new batch of messages
        // with which to make the next onEvents call. The pump gains nothing by running faster than onEvents.

        // The underlying client returns null if there are no events, but the contract for IEventProcessor
        // is different and is expecting an empty iterable if there are no events (and invoke processor after
        // receive timeout is turned on).
        
        Iterable<EventData> effectiveEvents = events;
        if (effectiveEvents == null)
        {
        	effectiveEvents = new ArrayList<EventData>();
        }
        
    	// Update offset and sequence number in the PartitionContext to support argument-less overload of PartitionContext.checkpoint()
		Iterator<EventData> iter = effectiveEvents.iterator();
		EventData last = null;
		while (iter.hasNext())
		{
			last = iter.next();
		}
		if (last != null)
		{
			this.partitionContext.setOffsetAndSequenceNumber(last);
		}
		
    	try
        {
        	// Synchronize to serialize calls to the processor.
        	// The handler is not installed until after onOpen returns, so onEvents cannot overlap with onOpen.
    		// onEvents and onClose are synchronized via this.processingSynchronizer to prevent calls to onClose
        	// while an onEvents call is still in progress.
        	synchronized(this.processingSynchronizer)
        	{
        		this.processor.onEvents(this.partitionContext, effectiveEvents);
        	}
        }
        catch (Exception e)
        {
            // TODO -- do we pass errors from IEventProcessor.onEvents to IEventProcessor.onError?
        	// Depending on how you look at it, that's either pointless (if the user's code throws, the user's code should already know about it) or
        	// a convenient way of centralizing error handling.
        	// In the meantime, just trace it.
        	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, this.partitionContext,
                    "Got exception from onEvents"), e);
        }
	}

	@Override
    public void onError(Throwable error)
    {
		if (error == null)
		{
			error = new Throwable("No error info supplied by EventHub client");
		}
		if (error instanceof ReceiverDisconnectedException)
		{
			TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host,
                    this.partitionContext,
					"EventHub client disconnected, probably another host took the partition"));
		}
		else
		{
            TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host,
                    this.partitionContext, "EventHub client error: " + error.toString()));
			if (error instanceof Exception)
			{
				TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host,
                        this.partitionContext, "EventHub client error continued"), (Exception)error);
			}
		}

		// It is vital to perform the rest of cleanup in a separate thread and not block this one. This thread is the client's
		// receive pump thread, and blocking it means that the receive pump never completes its CompletableFuture, which in turn
		// blocks other client calls that we would like to make during cleanup. Specifically, this issue was found when
		// PartitionReceiver.setReceiveHandler(null).get() was called and never returned.
		final Throwable capturedError = error;
		CompletableFuture.runAsync(() -> PartitionPump.this.processor.onError(PartitionPump.this.partitionContext, capturedError), this.host.getExecutorService())
			.thenRunAsync(() -> internalShutdown(CloseReason.Shutdown, capturedError), this.host.getExecutorService());
    }
}
