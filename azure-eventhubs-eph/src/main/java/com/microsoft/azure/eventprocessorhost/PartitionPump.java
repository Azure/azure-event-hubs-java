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
import java.util.concurrent.ExecutionException;
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
	protected final EventProcessorHost host;
	protected final Pump pump;
	protected Lease lease = null;

	private EventHubClient eventHubClient = null;
	private PartitionReceiver partitionReceiver = null;

	protected PartitionPumpStatus pumpStatus = PartitionPumpStatus.PP_UNINITIALIZED;

    private CompletableFuture<?> internalOperationFuture = null;
	
    protected IEventProcessor processor = null;
    protected PartitionContext partitionContext = null;
    
    protected final Object processingSynchronizer;
    
    protected ScheduledFuture<?> leaseRenewerFuture = null;

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(PartitionPump.class);
    
	PartitionPump(EventProcessorHost host, Pump pump, Lease lease)
	{
		super(host.getEventProcessorOptions().getMaxBatchSize());
		
		this.host = host;
		this.pump = pump;
		this.lease = lease;
		this.processingSynchronizer = new Object();
	}
	
	void setLease(Lease newLease)
	{
		this.partitionContext.setLease(newLease);
	}
	
    void startPump()
    {
    	this.pumpStatus = PartitionPumpStatus.PP_OPENING;
    	
        this.partitionContext = new PartitionContext(this.host, this.lease.getPartitionId(), this.host.getEventHubPath(), this.host.getConsumerGroupName());
        this.partitionContext.setLease(this.lease);
        
        if (this.pumpStatus == PartitionPumpStatus.PP_OPENING)
        {
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
            	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext, "Failed " + action), e);
            	this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), e, action, this.lease.getPartitionId());
            	
            	this.pumpStatus = PartitionPumpStatus.PP_OPENFAILED;
            }
        }

        if (this.pumpStatus == PartitionPumpStatus.PP_OPENING)
        {
        	specializedStartPump();
        }
        
        if (this.pumpStatus == PartitionPumpStatus.PP_RUNNING)
        {
			this.host.getExecutorService().schedule(() -> leaseRenewer(),
					this.host.getPartitionManagerOptions().getLeaseRenewIntervalInSeconds(), TimeUnit.SECONDS);
        }
        else
        {
            // There was an error in specialized startup, so clean up the processor.
            this.pumpStatus = PartitionPumpStatus.PP_CLOSING;
            try
            {
                this.processor.onClose(this.partitionContext, CloseReason.Shutdown);
            }
            catch (Exception e)
            {
                // If the processor fails on close, just log and notify.
                TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                        "Failed " + EventProcessorHostActionStrings.CLOSING_EVENT_PROCESSOR), e);
                this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), e, EventProcessorHostActionStrings.CLOSING_EVENT_PROCESSOR,
                   this.lease.getPartitionId());
            }
            this.processor = null;
            this.pumpStatus = PartitionPumpStatus.PP_CLOSED;
        }
    }

    void specializedStartPump()
    {
    	boolean openedOK = false;
    	int retryCount = 0;
    	Exception lastException = null;
    	do
    	{
	        try
	        {
				openClients();
				openedOK = true;
			}
	        catch (Exception e)
	        {
	        	lastException = e;
	        	if ((e instanceof ExecutionException) && (e.getCause() != null) && (e.getCause() instanceof ReceiverDisconnectedException))
	        	{
	        		// TODO Assuming this is due to a receiver with a higher epoch.
	        		// Is there a way to be sure without checking the exception text?
                    TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                            "Receiver disconnected on create, bad epoch?"), e);
	        		// If it's a bad epoch, then retrying isn't going to help.
	        		break;
	        	}
	        	else
	        	{
					TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                            "Failure creating client or receiver, retrying"), e);
					retryCount++;
	        	}
			}
    	} while (!openedOK && (retryCount < 5));
    	if (!openedOK)
    	{
            // IEventProcessor.onOpen is called from the base PartitionPump and must have returned in order for execution to reach here, 
    		// so we can report this error to it instead of the general error handler.
    		this.processor.onError(this.partitionContext, lastException);
			this.pumpStatus = PartitionPumpStatus.PP_OPENFAILED;
    	}

        if (this.pumpStatus == PartitionPumpStatus.PP_OPENING)
        {
            // IEventProcessor.onOpen is called from the base PartitionPump and must have returned in order for execution to reach here, 
            // meaning it is safe to set the handler and start calling IEventProcessor.onEvents.
            // Set the status to running before setting the javaClient handler, so the IEventProcessor.onEvents can never race and see status != running.
            this.pumpStatus = PartitionPumpStatus.PP_RUNNING;
            this.partitionReceiver.setReceiveHandler(this, this.host.getEventProcessorOptions().getInvokeProcessorAfterReceiveTimeout());
        }
        
        if (this.pumpStatus == PartitionPumpStatus.PP_OPENFAILED)
        {
        	this.pumpStatus = PartitionPumpStatus.PP_CLOSING;
        	cleanUpClients();
        	this.pumpStatus = PartitionPumpStatus.PP_CLOSED;
        }
    }

    private void openClients() throws EventHubException, IOException, InterruptedException, ExecutionException, ExceptionWithAction
    {
    	// Create new client
        TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext, "Opening EH client"));
		this.internalOperationFuture = EventHubClient.createFromConnectionString(this.host.getEventHubConnectionString());
		this.eventHubClient = (EventHubClient) this.internalOperationFuture.get();
		this.internalOperationFuture = null;
		
	    // Create new receiver and set options
        ReceiverOptions options = new ReceiverOptions();
        options.setReceiverRuntimeMetricEnabled(this.host.getEventProcessorOptions().getReceiverRuntimeMetricEnabled());
    	Object startAt = this.partitionContext.getInitialOffset();
    	long epoch = this.lease.getEpoch();
        TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                "Opening EH receiver with epoch " + epoch + " at location " + startAt));
    	if (startAt instanceof String)
    	{
    		this.internalOperationFuture = this.eventHubClient.createEpochReceiver(this.partitionContext.getConsumerGroupName(), this.partitionContext.getPartitionId(),
    				(String)startAt, epoch, options);
    	}
    	else if (startAt instanceof Instant) 
    	{
    		this.internalOperationFuture = this.eventHubClient.createEpochReceiver(this.partitionContext.getConsumerGroupName(), this.partitionContext.getPartitionId(),
    				(Instant)startAt, epoch, options);
    	}
    	else
    	{
    		String errMsg = "Starting offset is not String or Instant, is " + ((startAt != null) ? startAt.getClass().toString() : "null");
            TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext, errMsg));
    		throw new RuntimeException(errMsg);
    	}
		this.lease.setEpoch(epoch);
		if (this.internalOperationFuture != null)
		{
			this.partitionReceiver = (PartitionReceiver) this.internalOperationFuture.get();
			this.internalOperationFuture = null;
		}
		else
		{
            TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                    "createEpochReceiver failed with null"));
			throw new RuntimeException("createEpochReceiver failed with null");
		}
		this.partitionReceiver.setPrefetchCount(this.host.getEventProcessorOptions().getPrefetchCount());
		this.partitionReceiver.setReceiveTimeout(this.host.getEventProcessorOptions().getReceiveTimeOut());

        TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                "EH client and receiver creation finished"));
    }
    
    private void cleanUpClients() // swallows all exceptions
    {
        if (this.partitionReceiver != null)
        {
			// Disconnect the processor from the receiver we're about to close.
			// Fortunately this is idempotent -- setting the handler to null when it's already been
			// nulled by code elsewhere is harmless!
			// Setting to null also waits for the in-progress calls to complete
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                    "Setting receive handler to null"));
			try
			{
				this.partitionReceiver.setReceiveHandler(null).get();
			}
			catch (InterruptedException | ExecutionException e)
			{
				if (e instanceof InterruptedException)
				{
					// Re-assert the thread's interrupted status
					Thread.currentThread().interrupt();
				}

				final Throwable throwable = e.getCause();
				if (throwable != null)
				{
					TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
							"Got exception from onEvents when ReceiveHandler is set to null."), throwable);
				}
			}

            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext, "Closing EH receiver"));
        	final PartitionReceiver partitionReceiverTemp = this.partitionReceiver;
			this.partitionReceiver = null;

			try
			{
				partitionReceiverTemp.closeSync();
			}
			catch (EventHubException exception)
			{
                TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                        "Closing EH receiver failed."), exception);
			}
        }
        else
        {
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                    "partitionReceiver is null in cleanup"));
        }

        if (this.eventHubClient != null)
		{
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext, "Closing EH client"));
			final EventHubClient eventHubClientTemp = this.eventHubClient;
			this.eventHubClient = null;

			try
			{
				eventHubClientTemp.closeSync();
			}
			catch (EventHubException exception)
			{
				TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext, "Closing EH client failed."), exception);
			}
		}
        else
        {
            TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                    "eventHubClient is null in cleanup"));
        }
    }
    
    PartitionPumpStatus getPumpStatus()
    {
    	return this.pumpStatus;
    }
    
    Boolean isClosing()
    {
    	return ((this.pumpStatus == PartitionPumpStatus.PP_CLOSING) || (this.pumpStatus == PartitionPumpStatus.PP_CLOSED));
    }

    void shutdown(CloseReason reason)
    {
    	synchronized (this.pumpStatus)
    	{
    		// Make this method safe against races, for example it might be double-called in close succession if
    		// the partition is stolen, which results in a pump failure due to receiver disconnect, at about the
    		// same time as the PartitionManager is scanning leases.
    		if (isClosing())
    		{
    			return;
    		}
    		this.pumpStatus = PartitionPumpStatus.PP_CLOSING;
    	}
        TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
               "pump shutdown for reason " + reason.toString()));

        specializedShutdown(reason);

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
            	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                        "Failure closing processor"), e);
            	// If closing the processor has failed, the state of the processor is suspect.
            	// Report the failure to the general error handler instead.
            	this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), e, "Closing Event Processor", this.lease.getPartitionId());
            }
        }
        
        if (reason != CloseReason.LeaseLost)
        {
	        // Since this pump is dead, release the lease. Releasing can involve I/O and hence be slow, but
        	// shutdown doesn't need to wait for that, or care about any errors that may occur. Worst case is
        	// that the lease eventually expires, since the lease renewer has been cancelled. So execute the
        	// release async.
        	CompletableFuture.runAsync(new Runnable() {
					@Override
					public void run()
					{
			        	PartitionContext capturedContext = PartitionPump.this.partitionContext;
				        try
				        {
							PartitionPump.this.host.getLeaseManager().releaseLease(capturedContext.getLease());
						}
				        catch (ExceptionWithAction e)
				        {
				        	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(PartitionPump.this.host.getHostName(), capturedContext,
				        			"Failure releasing lease on pump shutdown"), e);
						}
					}
	        	}, this.host.getExecutorService());
        }
        // else we already lost the lease, releasing is unnecessary and would fail if we try
        
        this.pumpStatus = PartitionPumpStatus.PP_CLOSED;
    }
    
    void specializedShutdown(CloseReason reason)
    {
    	// If an open operation is stuck, this lets us shut down anyway.
    	CompletableFuture<?> captured = this.internalOperationFuture;
    	if (captured != null)
    	{
    		captured.cancel(true);
    	}
    	
    	if (this.partitionReceiver != null)
    	{
    		// Close the EH clients. Errors are swallowed, nothing we could do about them anyway.
            cleanUpClients();
    	}
    }
    
    // Returns Void so it can be used in a lambda.
    Void leaseRenewer()
    {
    	boolean scheduleNext = true;
    	
    	try
    	{
        	Lease captured = this.lease;
			if (!this.host.getLeaseManager().renewLease(captured))
			{
				// False return from renewLease means that lease was lost.
				// Start pump shutdown process and do not schedule another call to leaseRenewer.
				scheduleNext = false;
				this.host.getExecutorService().submit(() -> this.pump.removePump(this.partitionContext.getPartitionId(), CloseReason.LeaseLost));
			}
		}
    	catch (ExceptionWithAction e)
    	{
    		// Failure renewing lease due to storage exception or whatever.
    		// Trace error and leave scheduleNext as true to schedule another try.
    		Exception notifyWith = LoggingUtils.unwrapException(e, null);
    		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext, "Transient failure renewing lease"), notifyWith);
    		// Notify the general error handler rather than calling this.processor.onError so we can provide context (RENEWING_LEASE)
    		this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith, EventProcessorHostActionStrings.RENEWING_LEASE,
    				this.partitionContext.getPartitionId());
		}
    	
		if (scheduleNext)
		{
			this.host.getExecutorService().schedule(() -> leaseRenewer(),
					this.host.getPartitionManagerOptions().getLeaseRenewIntervalInSeconds(), TimeUnit.SECONDS);
		}
		
    	return null;
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
        	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), this.partitionContext,
                    "Got exception from onEvents"), e);
        }
	}

	@Override
    public void onError(Throwable error)
    {
		this.pumpStatus = PartitionPumpStatus.PP_ERRORED;
		if (error == null)
		{
			error = new Throwable("No error info supplied by EventHub client");
		}
		if (error instanceof ReceiverDisconnectedException)
		{
			TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(),
                    this.partitionContext,
					"EventHub client disconnected, probably another host took the partition"));
		}
		else
		{
            TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(),
                    this.partitionContext, "EventHub client error: " + error.toString()));
			if (error instanceof Exception)
			{
				TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(),
                        this.partitionContext, "EventHub client error continued"), (Exception)error);
			}
		}

		// It is vital to perform the rest of cleanup in a separate thread and not block this one. This thread is the client's
		// receive pump thread, and blocking it means that the receive pump never completes its CompletableFuture, which in turn
		// blocks other client calls that we would like to make during cleanup. Specifically, this issue was found when
		// PartitionReceiver.setReceiveHandler(null).get() was called and never returned.
		final Throwable capturedError = error;
		PartitionPump.this.host.getExecutorService().submit(new Runnable() {
			@Override
			public void run()
			{
		    	// Notify the user's IEventProcessor
		    	PartitionPump.this.processor.onError(PartitionPump.this.partitionContext, capturedError);
		    	
		    	// Notify upstream that this pump is dead so that cleanup will occur.
		    	// Failing to do so results in reactor threads leaking.
		    	PartitionPump.this.pump.onPumpError(PartitionPump.this.partitionContext.getPartitionId());
			}
		});
    }
}
