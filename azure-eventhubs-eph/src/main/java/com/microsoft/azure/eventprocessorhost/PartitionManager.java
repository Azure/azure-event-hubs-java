/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    protected String partitionIds[] = null;
    
    private ScheduledFuture<?> scanFuture = null;

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(PartitionManager.class);

    PartitionManager(EventProcessorHost host)
    {
        this.host = host;
    }
    
    CompletableFuture<String[]> getPartitionIds()
    {
    	CompletableFuture<String[]> retval = null;
    	
    	if (this.partitionIds != null)
    	{
    		retval = CompletableFuture.completedFuture(this.partitionIds);
    	}
    	else
        {
    		try
    		{
				retval = EventHubClient.createFromConnectionString(this.host.getEventHubConnectionString())
				.thenComposeAsync((ehClient) -> ehClient.getRuntimeInformation(), this.host.getExecutorService())
				.thenAccept((ehInfo) ->
				{
					if (ehInfo != null)
					{
						this.partitionIds = ehInfo.getPartitionIds();

						TRACE_LOGGER.info(LoggingUtils.withHost(this.host,
				               "Eventhub " + this.host.getEventHubPath() + " count of partitions: " + ehInfo.getPartitionCount()));
						for (String id : this.partitionIds)
						{
							TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Found partition with id: " + id));
						}
					}
					else
					{
						throw new CompletionException(new TimeoutException("getRuntimeInformation returned null"));
					}
				})
				.handleAsync((empty, e) ->
				{
					if (e != null)
					{
						throw new CompletionException(new IllegalEntityException("Failure getting partition ids for event hub", e));
					}
					return this.partitionIds;
				});
			}
    		catch (EventHubException | IOException e)
    		{
    			retval = new CompletableFuture<String[]>();
    			retval.completeExceptionally(new CompletionException(new IllegalEntityException("Failure getting partition ids for event hub", e)));
			}
        }
        
        return retval;
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
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Shutting down all pumps"));
    	Iterable<CompletableFuture<Void>> pumpRemovals = this.pump.removeAllPumps(CloseReason.Shutdown);
    	for (Future<?> removal : pumpRemovals)
    	{
    		try
    		{
				removal.get();
			}
    		catch (InterruptedException | ExecutionException e)
    		{
    			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, "Failure during shutdown"), e);
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

        TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Partition manager exiting"));
    }
    
    public CompletableFuture<Void> initialize()
    {
    	this.pump = createPumpTestHook();
    	
    	return getPartitionIds()
    	.thenComposeAsync((unused) -> initializeStores(), this.host.getExecutorService()) 
    	.whenCompleteAsync((empty, e) ->
    	{
    		if (e != null)
    		{
    			StringBuilder outAction = new StringBuilder();
    			Throwable inner = LoggingUtils.unwrapException(e, outAction);
    			if (outAction.length() > 0)
    			{
    	    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host,
    	                    "Exception while initializing stores (" + outAction.toString() + "), not starting partition manager"), inner);
    			}
    			else
    			{
    	    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host,
    	                    "Exception while initializing stores, not starting partition manager"), inner);
    			}
    			throw new CompletionException(inner);
    		}
    	}, this.host.getExecutorService())
    	.thenRunAsync(() ->
    	{
			// Schedule the first scan right away.
	    	this.scanFuture = this.host.getExecutorService().schedule(() -> scan(), 0, TimeUnit.SECONDS);
	    	
			onInitializeCompleteTestHook();
    	}, this.host.getExecutorService());
    }
    
    private CompletableFuture<Void> initializeStores()
    {
        ILeaseManager leaseManager = this.host.getLeaseManager();
        ICheckpointManager checkpointManager = this.host.getCheckpointManager();

        return leaseManager.leaseStoreExists().thenAcceptAsync((exists) ->
        {
        	// If the lease store doesn't exist, create it
        	if (!exists)
        	{
	        	try
	        	{
					retryWrapper(() -> leaseManager.createLeaseStoreIfNotExists(), null, "Failure creating lease store for this Event Hub, retrying",
							"Out of retries creating lease store for this Event Hub", EventProcessorHostActionStrings.CREATING_LEASE_STORE, 5);
				}
	        	catch (Exception e)
	        	{
	        		throw LoggingUtils.wrapException(e, EventProcessorHostActionStrings.CREATING_LEASE_STORE);
				}
        	}
        }, this.host.getExecutorService())
        .thenRunAsync(() ->
        {
        	// If we get here, the lease store exists. Now create the leases if necessary.
            for (String id : this.partitionIds)
            {
            	try
            	{
					retryWrapper(() -> leaseManager.createLeaseIfNotExists(id), id, "Failure creating lease for partition, retrying",
							"Out of retries creating lease for partition", EventProcessorHostActionStrings.CREATING_LEASE, 5);
				}
            	catch (Exception e)
            	{
            		throw LoggingUtils.wrapException(e, EventProcessorHostActionStrings.CREATING_LEASE);
				}
            }
        }, this.host.getExecutorService())
        .thenComposeAsync((empty) -> checkpointManager.checkpointStoreExists(), this.host.getExecutorService())
        .thenAcceptAsync((exists) ->
        {
        	// If the checkpoint store doesn't exist, create it
        	if (!exists)
            {
        		try
        		{
	            	retryWrapper(() -> checkpointManager.createCheckpointStoreIfNotExists(), null, "Failure creating checkpoint store for this Event Hub, retrying",
	            			"Out of retries creating checkpoint store for this Event Hub", EventProcessorHostActionStrings.CREATING_CHECKPOINT_STORE, 5);
				}
	        	catch (Exception e)
	        	{
	        		throw LoggingUtils.wrapException(e, EventProcessorHostActionStrings.CREATING_CHECKPOINT_STORE);
				}
            }
        	
        	// If we get here, the checkpoint store exists. Now create the checkpoint holders, if needed.
            for (String id : this.partitionIds)
            {
            	try
            	{
	            	retryWrapper(() -> checkpointManager.createCheckpointIfNotExists(id), id, "Failure creating checkpoint for partition, retrying",
	            			"Out of retries creating checkpoint blob for partition", EventProcessorHostActionStrings.CREATING_CHECKPOINT, 5);
				}
	        	catch (Exception e)
	        	{
	        		throw LoggingUtils.wrapException(e, EventProcessorHostActionStrings.CREATING_CHECKPOINT);
				}
            }
        }, this.host.getExecutorService());
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
    				TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId, retryMessage), e);
    			}
    			else
    			{
    				TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, retryMessage), e);
    			}
    			retryCount++;
    		}
    	} while (!createdOK && (retryCount < maxRetries));
    	if (!createdOK)
        {
    		if (partitionId != null)
    		{
    			TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId, finalFailureMessage));
    		}
    		else
    		{
    			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, finalFailureMessage));
    		}
    		throw new ExceptionWithAction(new RuntimeException(finalFailureMessage), action);
        }
    }
    
    // Return Void so it can be called from a lambda.
    // throwOnFailure is true 
    private Void scan()
    {
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Starting lease scan"));

    	// DO NOT check whether this.scanFuture is cancelled. The first execution of this method is scheduled
    	// with 0 delay and can occur before this.scanFuture is set to the result of the schedule() call.

    	try
    	{
            List<CompletableFuture<Lease>> gettingAllLeases = this.host.getLeaseManager().getAllLeases(this.partitionIds);

            ArrayList<CompletableFuture<Lease>> allLeasesResults = new ArrayList<CompletableFuture<Lease>>();
            for (CompletableFuture<Lease> leaseFuture : gettingAllLeases)
            {
            	CompletableFuture<Lease> oneResult = leaseFuture.thenApplyAsync((Lease possibleLease) ->
        		{
					try
					{
						if (possibleLease.isExpired().get())
						{
							if (this.host.getLeaseManager().acquireLease(possibleLease).get())
							{
								this.pump.addPump(possibleLease);
							}
						}
					}
					catch (Exception e)
					{
						throw new CompletionException(e);
					}
                    return possibleLease;
        		}, this.host.getExecutorService());
            	allLeasesResults.add(oneResult);
            }
            
            ArrayList<Lease> leasesOwnedByOthers = new ArrayList<Lease>();
            int ourLeasesCount = 0;
            boolean completeResults = true;
            for (CompletableFuture<Lease> leaseResult : allLeasesResults)
            {
            	Lease result = null;
            	try
            	{
	            	result = leaseResult.get();
	            	if (result.isOwnedBy(this.host.getHostName()))
	            	{
	            		ourLeasesCount++;
	            	}
	            	else
	            	{
	            		leasesOwnedByOthers.add(result);
	            	}
            	}
            	// Catch exceptions from processing of individual leases here so that all leases are touched.
            	catch (CompletionException|ExecutionException e)
            	{
            		completeResults = false;
            		Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, null);
            		String partitionId = (result != null) ? result.getPartitionId() : ExceptionReceivedEventArgs.NO_ASSOCIATED_PARTITION;
            		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId, "Failure getting/acquiring lease, skipping"),
            				notifyWith);
            		this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith, EventProcessorHostActionStrings.CHECKING_LEASES,
            				partitionId);
            	}
            }
            TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Our leases: " + ourLeasesCount + "   other leases: " + leasesOwnedByOthers.size()));
            
            // Grab more leases if available and needed for load balancing, but only if all leases were checked OK.
            // Don't try to steal if numbers are in doubt due to errors in the previous stage. 
            if ((leasesOwnedByOthers.size() > 0) && completeResults)
            {
	            Lease stealThisLease = whichLeaseToSteal(leasesOwnedByOthers, ourLeasesCount);
	            if (stealThisLease != null)
	            {
            		try
            		{
	                	if (this.host.getLeaseManager().acquireLease(stealThisLease).get())
	                	{
	                		TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, stealThisLease, "Stole lease"));
	                		ourLeasesCount++;
                    		this.pump.addPump(stealThisLease);
	                	}
	                	else
	                	{
	                		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host,
                                    "Failed to steal lease for partition " + stealThisLease.getPartitionId()));
	                	}
            		}
            		// Catch stealing exceptions here to give a better error message
            		catch (Exception e)
            		{
            			Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, null);
            			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host,
                                "Exception stealing lease for partition " + stealThisLease.getPartitionId()), notifyWith);
            			this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith,
            					EventProcessorHostActionStrings.STEALING_LEASE, stealThisLease.getPartitionId());
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
    	catch (CompletionException e)
    	{
    		StringBuilder action = new StringBuilder();
    		Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, action);
    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, "Failure checking leases"), notifyWith);
    		this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith,
    				(action.length() > 0) ? action.toString() : EventProcessorHostActionStrings.CHECKING_LEASES);
    	}

    	// Schedule the next scan unless the future has been cancelled.
    	if (!this.scanFuture.isCancelled())
    	{
    		int seconds = this.host.getPartitionManagerOptions().getLeaseRenewIntervalInSeconds();
	    	this.scanFuture = this.host.getExecutorService().schedule(() -> scan(), seconds, TimeUnit.SECONDS);
	    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Scheduling lease scanner in " + seconds));
    	}
    	
    	return null;
    }

    private Lease whichLeaseToSteal(ArrayList<Lease> stealableLeases, int haveLeaseCount)
    {
    	HashMap<String, Integer> countsByOwner = countLeasesByOwner(stealableLeases);
    	String biggestOwner = findBiggestOwner(countsByOwner);
    	int biggestCount = countsByOwner.get(biggestOwner);
    	Lease stealThisLease = null;
    	
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
    		for (Lease l : stealableLeases)
    		{
    			if (l.isOwnedBy(biggestOwner))
    			{
    				stealThisLease = l;
    				TRACE_LOGGER.info(LoggingUtils.withHost(this.host,
                            "Proposed to steal lease for partition " + l.getPartitionId() + " from " + biggestOwner));
  					break;
    			}
    		}
    	}
    	return stealThisLease;
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
    		TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "host " + owner + " owns " + counts.get(owner) + " leases"));
    	}
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "total hosts in sorted list: " + counts.size()));
    	
    	return counts;
    }
}
