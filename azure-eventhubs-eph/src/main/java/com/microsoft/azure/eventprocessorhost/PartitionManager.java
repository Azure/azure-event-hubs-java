/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventhubs.IllegalEntityException;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionManager
{
	// Protected instead of private for testability
    protected final EventProcessorHost host;
    protected Pump pump;

    private String partitionIds[] = null;
    
    private ScheduledFuture<?> scanFuture = null;

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(PartitionManager.class);

    PartitionManager(EventProcessorHost host)
    {
        this.host = host;
    }
    
    String[] getPartitionIds() throws IllegalEntityException
    {
        Throwable saved = null;

        if (this.partitionIds == null)
        {
			try
			{
				EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(this.host.getEventHubConnectionString());
				EventHubRuntimeInformation ehInfo = ehClient.getRuntimeInformation().get();
				if (ehInfo != null)
				{
					this.partitionIds = ehInfo.getPartitionIds();
	
					TRACE_LOGGER.info(LoggingUtils.withHost(this.host.getHostName(),
                           "Eventhub " + this.host.getEventHubPath() + " count of partitions: " + ehInfo.getPartitionCount()));
					for (String id : this.partitionIds)
					{
						TRACE_LOGGER.info(LoggingUtils.withHost(this.host.getHostName(), "Found partition with id: " + id));
					}
				}
				else
				{
					saved = new TimeoutException("getRuntimeInformation returned null");
				}
			}
			catch (EventHubException | IOException | InterruptedException | ExecutionException e)
			{
				saved = e;
			}
        }
        if (this.partitionIds == null)
        {
			throw new IllegalEntityException("Failure getting partition ids for event hub", saved);
        }
        
        return this.partitionIds;
    }

    // Testability hook: allows a test subclass to insert dummy pump.
    Pump createPumpTestHook()
    {
        return new Pump(this.host);
    }

    // Testability hook: called after stores are initialized.
    void onInitializeCompleteTestHook()
    {
    }

    // Testability hook: called at the end of the main loop after all partition checks/stealing is complete.
    void onPartitionCheckCompleteTestHook()
    {
    }
    
    void stopPartitions()
    {
    	// Stop the lease scanner.
    	ScheduledFuture<?> captured = this.scanFuture;
    	if (captured != null)
    	{
    		captured.cancel(true);
    	}

    	// Stop any partition pumps that are running.
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host.getHostName(), "Shutting down all pumps"));
    	Iterable<Future<?>> pumpRemovals = this.pump.removeAllPumps(CloseReason.Shutdown);
    	for (Future<?> removal : pumpRemovals)
    	{
    		try
    		{
				removal.get();
			}
    		catch (InterruptedException | ExecutionException e)
    		{
    			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host.getHostName(), "Failure during shutdown"), e);
    			this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), e, EventProcessorHostActionStrings.PARTITION_MANAGER_CLEANUP);
    			
    			// By convention, bail immediately on interrupt, even though we're just cleaning
    			// up on the way out. Fortunately, we ARE just cleaning up on the way out, so we're
    			// free to bail without serious side effects.
    			if (e instanceof InterruptedException)
    			{
    				Thread.currentThread().interrupt();
    				throw new RuntimeException(e);
    			}
			}
    	}

        TRACE_LOGGER.info(LoggingUtils.withHost(this.host.getHostName(), "Partition manager exiting"));
    }
    
    // Return Void so it can be called from a lambda.
    public Void initialize() throws CompletionException
    {
    	this.pump = createPumpTestHook();
    	
    	try
    	{
    		initializeStores();
    		
    		// Schedule the first scan right away.
        	this.scanFuture = this.host.getExecutorService().schedule(() -> scan(), 0, TimeUnit.SECONDS);
        	
    		onInitializeCompleteTestHook();
    	}
    	catch (ExceptionWithAction e)
    	{
    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host.getHostName(),
                    "Exception while initializing stores (" + e.getAction() + "), not starting partition manager"), e.getCause());
    		throw new CompletionException(e);
    	}
    	catch (Exception e)
    	{
    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host.getHostName(),
                    "Exception while initializing stores, not starting partition manager"), e);
    		throw new CompletionException(e);
    	}
    	
    	return null;
    }
    
    private void initializeStores() throws ExceptionWithAction, IllegalEntityException
    {
        ILeaseManager leaseManager = this.host.getLeaseManager();
        
        // Make sure the lease store exists
        if (!leaseManager.leaseStoreExists())
        {
        	retryWrapper(() -> leaseManager.createLeaseStoreIfNotExists(), null, "Failure creating lease store for this Event Hub, retrying",
        			"Out of retries creating lease store for this Event Hub", EventProcessorHostActionStrings.CREATING_LEASE_STORE, 5);
        }
        // else
        //	lease store already exists, no work needed
        
        // Now make sure the leases exist
        for (String id : getPartitionIds())
        {
        	retryWrapper(() -> leaseManager.createLeaseIfNotExists(id), id, "Failure creating lease for partition, retrying",
        			"Out of retries creating lease for partition", EventProcessorHostActionStrings.CREATING_LEASE, 5);
        }
        
        ICheckpointManager checkpointManager = this.host.getCheckpointManager();
        
        // Make sure the checkpoint store exists
        if (!checkpointManager.checkpointStoreExists())
        {
        	retryWrapper(() -> checkpointManager.createCheckpointStoreIfNotExists(), null, "Failure creating checkpoint store for this Event Hub, retrying",
        			"Out of retries creating checkpoint store for this Event Hub", EventProcessorHostActionStrings.CREATING_CHECKPOINT_STORE, 5);
        }
        // else
        //	checkpoint store already exists, no work needed
        
        // Now make sure the checkpoints exist
        for (String id : getPartitionIds())
        {
        	retryWrapper(() -> checkpointManager.createCheckpointIfNotExists(id), id, "Failure creating checkpoint for partition, retrying",
        			"Out of retries creating checkpoint blob for partition", EventProcessorHostActionStrings.CREATING_CHECKPOINT, 5);
        }
    }
    
    // Throws if it runs out of retries. If it returns, action succeeded.
    private void retryWrapper(Callable<?> lambda, String partitionId, String retryMessage, String finalFailureMessage, String action, int maxRetries) throws ExceptionWithAction
    {
    	boolean createdOK = false;
    	int retryCount = 0;
    	do
    	{
    		try
    		{
    			lambda.call();
    			createdOK = true;
    		}
    		catch (Exception e)
    		{
    			if (partitionId != null)
    			{
    				TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), partitionId, retryMessage), e);
    			}
    			else
    			{
    				TRACE_LOGGER.warn(LoggingUtils.withHost(this.host.getHostName(), retryMessage), e);
    			}
    			retryCount++;
    		}
    	} while (!createdOK && (retryCount < maxRetries));
    	if (!createdOK)
        {
    		if (partitionId != null)
    		{
    			TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), partitionId, finalFailureMessage));
    		}
    		else
    		{
    			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host.getHostName(), finalFailureMessage));
    		}
    		throw new ExceptionWithAction(new RuntimeException(finalFailureMessage), action);
        }
    }
    
    private enum LeaseState { START, EXPIRED, ACQUIRED, FINAL_OURS, FINAL_NOT_OURS };
    private class LeaseStateHolder
    {
    	public LeaseState state;
    	public Lease lease;
    	
    	public LeaseStateHolder(Lease l)
    	{
    		this.state = LeaseState.START;
    		this.lease = l;
    	}
    }
    
    // Return Void so it can be called from a lambda.
    // throwOnFailure is true 
    private Void scan()
    {
    	// Theoretically, if the future is cancelled then this method should never fire, but
    	// there's no harm in being sure.
    	if (this.scanFuture.isCancelled())
    	{
    		return null;
    	}
    	
    	try
    	{
            ArrayList<CompletableFuture<Lease>> gettingAllLeases = this.host.getLeaseManager().getAllLeases();

            ArrayList<CompletableFuture<LeaseStateHolder>> allLeasesResults = new ArrayList<CompletableFuture<LeaseStateHolder>>();
            for (CompletableFuture<Lease> leaseFuture : gettingAllLeases)
            {
            	CompletableFuture<LeaseStateHolder> oneResult = leaseFuture.thenApplyAsync((Lease possibleLease) ->
            		{
            			LeaseStateHolder lsh = new LeaseStateHolder(possibleLease);
						try
						{
							if (possibleLease.isExpired())
							{
								lsh.state = LeaseState.EXPIRED;
							}
							else if (possibleLease.getOwner().compareTo(this.host.getHostName()) == 0)
							{
								lsh.state = LeaseState.FINAL_OURS;
							}
							else
							{
								lsh.state = LeaseState.FINAL_NOT_OURS;
							}
						}
						catch (Exception e)
						{
							throw new CompletionException(e);
						}
                        return lsh;
            		}, this.host.getExecutorService()).
            	thenApplyAsync((LeaseStateHolder lsh) ->
            		{
            			if (lsh.state == LeaseState.EXPIRED)
            			{
            				try
            				{
								if (this.host.getLeaseManager().acquireLease(lsh.lease))
								{
									lsh.state = LeaseState.ACQUIRED;
								}
								else
								{
									// Failed because another host acquired it between our get and acquire.
									lsh.state = LeaseState.FINAL_NOT_OURS;
								}
							}
            				catch (ExceptionWithAction e)
            				{
            					throw new CompletionException(e);
							}
            			}
            			return lsh;
            		}, this.host.getExecutorService()).
            	thenApplyAsync((LeaseStateHolder lsh) ->
            		{
            			if (lsh.state == LeaseState.ACQUIRED)
            			{
            				try
            				{
								this.pump.addPump(lsh.lease);
							}
            				catch (Exception e)
            				{
            					throw new CompletionException(e);
							}
            				lsh.state = LeaseState.FINAL_OURS;
            			}
            			return lsh;
            		}, this.host.getExecutorService());
            	allLeasesResults.add(oneResult);
            }
            
            ArrayList<Lease> leasesOwnedByOthers = new ArrayList<Lease>();
            int ourLeasesCount = 0;
            boolean completeResults = true;
            for (CompletableFuture<LeaseStateHolder> leaseResult : allLeasesResults)
            {
            	LeaseStateHolder result = null;
            	try
            	{
	            	result = leaseResult.get();
	            	if (result.state == LeaseState.FINAL_OURS)
	            	{
	            		ourLeasesCount++;
	            	}
	            	else if (result.state == LeaseState.FINAL_NOT_OURS)
	            	{
	            		leasesOwnedByOthers.add(result.lease);
	            	}
	            	else
	            	{
	            		// Should never happen. Log and skip.
	            		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), result.lease.getPartitionId(),
	            				"Bad state getting/acquiring lease, skipping"));
	            	}
            	}
            	// Catch exceptions from processing of individual leases here so that all leases are touched.
            	catch (CompletionException|ExecutionException e)
            	{
            		completeResults = false;
            		Exception notifyWith = LoggingUtils.unwrapException(e, null);
            		String partitionId = (result != null) ? result.lease.getPartitionId() : ExceptionReceivedEventArgs.NO_ASSOCIATED_PARTITION;
            		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host.getHostName(), partitionId, "Failure getting/acquiring lease, skipping"),
            				notifyWith);
            		this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith, EventProcessorHostActionStrings.CHECKING_LEASES,
            				partitionId);
            	}
            }
            
            // Grab more leases if available and needed for load balancing, but only if all leases were checked OK.
            // Don't try to steal if numbers are in doubt due to errors in the previous stage. 
            if ((leasesOwnedByOthers.size() > 0) && completeResults)
            {
	            Iterable<Lease> stealTheseLeases = whichLeasesToSteal(leasesOwnedByOthers, ourLeasesCount);
	            if (stealTheseLeases != null)
	            {
	            	for (Lease stealee : stealTheseLeases)
	            	{
	            		try
	            		{
    	                	if (this.host.getLeaseManager().acquireLease(stealee))
    	                	{
    	                		TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host.getHostName(), stealee.getPartitionId(),
                                        "Stole lease"));
    	                		ourLeasesCount++;
                        		this.pump.addPump(stealee);
    	                	}
    	                	else
    	                	{
    	                		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host.getHostName(),
                                        "Failed to steal lease for partition " + stealee.getPartitionId()));
    	                	}
	            		}
	            		// Catch stealing exceptions here so that if one steal fails, others are at least attempted.
	            		catch (Exception e)
	            		{
	            			Exception notifyWith = LoggingUtils.unwrapException(e, null);
	            			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host.getHostName(),
                                    "Exception stealing lease for partition " + stealee.getPartitionId()), notifyWith);
	            			this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith,
	            					EventProcessorHostActionStrings.STEALING_LEASE, stealee.getPartitionId());
	            		}
	            	}
	            }
            }

            onPartitionCheckCompleteTestHook();
    	}
    	// Catch top-level errors, which mean we have to give up on processing any leases this time around.
    	catch (InterruptedException ie)
    	{
    		// Shutdown time. Make sure the future is cancelled.
    		this.scanFuture.cancel(false);
    		Thread.currentThread().interrupt();
    	}
    	catch (Exception e)
    	{
    		StringBuilder action = new StringBuilder();
    		Exception notifyWith = LoggingUtils.unwrapException(e, action);
    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host.getHostName(), "Failure checking leases"), notifyWith);
    		this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith,
    				(action.length() > 0) ? action.toString() : EventProcessorHostActionStrings.CHECKING_LEASES);
    	}

    	// Schedule the next scan unless the future has been cancelled.
    	if (!this.scanFuture.isCancelled())
    	{
	    	this.scanFuture = this.host.getExecutorService().schedule(() -> scan(),
	    			this.host.getPartitionManagerOptions().getLeaseRenewIntervalInSeconds(), TimeUnit.SECONDS);
    	}
    	
    	return null;
    }

    private Iterable<Lease> whichLeasesToSteal(ArrayList<Lease> stealableLeases, int haveLeaseCount)
    {
    	HashMap<String, Integer> countsByOwner = countLeasesByOwner(stealableLeases);
    	String biggestOwner = findBiggestOwner(countsByOwner);
    	int biggestCount = countsByOwner.get(biggestOwner);
    	ArrayList<Lease> stealTheseLeases = null;
    	
    	// If the number of leases is a multiple of the number of hosts, then the desired configuration is
    	// that all hosts own the name number of leases, and the difference between the "biggest" owner and
    	// any other is 0.
    	//
    	// If the number of leases is not a multiple of the number of hosts, then the most even configuration
    	// possible is for some hosts to have (leases/hosts) leases and others to have ((leases/hosts) + 1).
    	// For example, for 16 partitions distributed over five hosts, the distribution would be 4, 3, 3, 3, 3,
    	// or any of the possible reorderings.
    	//
    	// In either case, if the difference between this host and the biggest owner is 2 or more, then the
    	// system is not in the most evenly-distributed configuration, so steal one lease from the biggest.
    	// If there is a tie for biggest, findBiggestOwner() picks whichever appears first in the list because
    	// it doesn't really matter which "biggest" is trimmed down.
    	//
    	// Stealing one at a time prevents flapping because it reduces the difference between the biggest and
    	// this host by two at a time. If the starting difference is two or greater, then the difference cannot
    	// end up below 0. This host may become tied for biggest, but it cannot become larger than the host that
    	// it is stealing from.
    	
    	if ((biggestCount - haveLeaseCount) >= 2)
    	{
    		stealTheseLeases = new ArrayList<Lease>();
    		for (Lease l : stealableLeases)
    		{
    			if (l.getOwner().compareTo(biggestOwner) == 0)
    			{
    				stealTheseLeases.add(l);
    				TRACE_LOGGER.info(LoggingUtils.withHost(this.host.getHostName(),
                            "Proposed to steal lease for partition " + l.getPartitionId() + " from " + biggestOwner));
  					break;
    			}
    		}
    	}
    	return stealTheseLeases;
    }
    
    private String findBiggestOwner(HashMap<String, Integer> countsByOwner)
    {
    	int biggestCount = 0;
    	String biggestOwner = null;
    	for (String owner : countsByOwner.keySet())
    	{
    		if (countsByOwner.get(owner) > biggestCount)
    		{
    			biggestCount = countsByOwner.get(owner);
    			biggestOwner = owner;
    		}
    	}
    	return biggestOwner;
    }
    
    private HashMap<String, Integer> countLeasesByOwner(Iterable<Lease> leases)
    {
    	HashMap<String, Integer> counts = new HashMap<String, Integer>();
    	for (Lease l : leases)
    	{
    		if (counts.containsKey(l.getOwner()))
    		{
    			Integer oldCount = counts.get(l.getOwner());
    			counts.put(l.getOwner(), oldCount + 1);
    		}
    		else
    		{
    			counts.put(l.getOwner(), 1);
    		}
    	}
    	for (String owner : counts.keySet())
    	{
    		TRACE_LOGGER.info("host " + owner + " owns " + counts.get(owner) + " leases");
    	}
    	TRACE_LOGGER.info("total hosts in sorted list: " + counts.size());
    	
    	return counts;
    }
}
