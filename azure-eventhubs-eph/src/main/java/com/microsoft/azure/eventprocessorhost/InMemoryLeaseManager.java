/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.IllegalEntityException;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/***
 * An ILeaseManager implementation based on an in-memory store. 
 *
 * THIS CLASS IS PROVIDED AS A CONVENIENCE FOR TESTING ONLY. All data stored via this class is in memory
 * only and not persisted in any way. In addition, it is only visible within the same process: multiple
 * instances of EventProcessorHost in the same process will share the same in-memory store and leases
 * created by one will be visible to the others, but that is not true across processes.
 * 
 * With an ordinary store, there is a clear and distinct line between the values that are persisted
 * and the values that are live in memory. With an in-memory store, that line gets blurry. If we
 * accidentally hand out a reference to the in-store object, then the calling code is operating on
 * the "persisted" values without going through the manager and behavior will be very different.
 * Hence, the implementation takes pains to distinguish between references to "live" and "persisted"
 * checkpoints.
 * 
 * To use this class, create a new instance and pass it to the EventProcessorHost constructor that takes
 * ILeaseManager as an argument. After the EventProcessorHost instance is constructed, be sure to
 * call initialize() on this object before starting processing with EventProcessorHost.registerEventProcessor()
 * or EventProcessorHost.registerEventProcessorFactory().
 */
public class InMemoryLeaseManager implements ILeaseManager
{
    private EventProcessorHost host;

    private final static Logger TRACE_LOGGER = LoggerFactory.getLogger(InMemoryLeaseManager.class);

    public InMemoryLeaseManager()
    {
    }

    // This object is constructed before the EventProcessorHost and passed as an argument to
    // EventProcessorHost's constructor. So it has to get a reference to the EventProcessorHost later.
    public void initialize(EventProcessorHost host)
    {
        this.host = host;
    }

    @Override
    public int getLeaseRenewIntervalInMilliseconds()
    {
    	return this.host.getPartitionManagerOptions().getLeaseRenewIntervalInSeconds() * 1000;
    }
    
    @Override
    public int getLeaseDurationInMilliseconds()
    {
    	return this.host.getPartitionManagerOptions().getLeaseDurationInSeconds() * 1000;
    }

    @Override
    public boolean leaseStoreExists()
    {
    	boolean exists = InMemoryLeaseStore.singleton.existsMap();
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "leaseStoreExists() " + exists));
    	return exists;
    }

    @Override
    public Void createLeaseStoreIfNotExists()
    {
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "createLeaseStoreIfNotExists()"));
    	InMemoryLeaseStore.singleton.initializeMap(getLeaseDurationInMilliseconds());
        return null;
    }
    
    @Override
    public boolean deleteLeaseStore()
    {
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "deleteLeaseStore()"));
    	InMemoryLeaseStore.singleton.deleteMap();
    	return true;
    }
    
    @Override
    public Lease getLease(String partitionId)
    {
    	InMemoryLease returnLease = null;
    	InMemoryLease leaseInStore = InMemoryLeaseStore.singleton.getLease(partitionId);
        if (leaseInStore == null)
        {
        	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId, "getLease() no existing lease"));
        	returnLease = null;
        }
        else
        {
        	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, partitionId, "getLease() found"));
        	returnLease = new InMemoryLease(leaseInStore);
        }
        return returnLease;
    }

    @Override
    public ArrayList<CompletableFuture<Lease>> getAllLeases() throws ExceptionWithAction
    {
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "getAllLeases()"));
        ArrayList<CompletableFuture<Lease>> leases = new ArrayList<CompletableFuture<Lease>>();
        
        String[] partitionIds = null;
        try
        {
        	partitionIds = this.host.getPartitionManager().getPartitionIds();
        }
        catch (IllegalEntityException e)
        {
    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, "Failure while getting details of all leases"), e);
    		throw new ExceptionWithAction(e, EventProcessorHostActionStrings.GETTING_LEASE);
        }

        for (String id : partitionIds)
        {
        	// getLease() is a fast, nonblocking action in this implementation.
        	// No need to actually be async, just fill the array with already-completed futures.
            leases.add(CompletableFuture.completedFuture(getLease(id)));
        }
        
        return leases;
    }

    @Override
    public Lease createLeaseIfNotExists(String partitionId)
    {
    	InMemoryLease leaseInStore = InMemoryLeaseStore.singleton.getLease(partitionId);
    	InMemoryLease returnLease = null;
        if (leaseInStore != null)
        {
        	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, partitionId,
                    "createLeaseIfNotExists() found existing lease, OK"));
        	returnLease = new InMemoryLease(leaseInStore);
        }
        else
        {
        	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, partitionId,
                    "createLeaseIfNotExists() creating new lease"));
        	InMemoryLease newStoreLease = new InMemoryLease(partitionId);
            newStoreLease.setEpoch(0L);
            newStoreLease.setOwner("");
            InMemoryLeaseStore.singleton.setOrReplaceLease(newStoreLease);
            returnLease = new InMemoryLease(newStoreLease);
        }
        return returnLease;
    }
    
    @Override
    public void deleteLease(Lease lease)
    {
    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, lease, "deleteLease()"));
    	InMemoryLeaseStore.singleton.removeLease((InMemoryLease)lease);
    }

    @Override
    public boolean acquireLease(Lease lease)
    {
    	InMemoryLease leaseToAcquire = (InMemoryLease)lease;
    	
    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, leaseToAcquire, "acquireLease()"));
    	
    	boolean retval = true;
    	InMemoryLease leaseInStore = InMemoryLeaseStore.singleton.getLease(leaseToAcquire.getPartitionId());
        if (leaseInStore != null)
        {
        	InMemoryLease wasUnowned = InMemoryLeaseStore.singleton.atomicAquireUnowned(leaseToAcquire.getPartitionId(), this.host.getHostName());
            if (wasUnowned != null)
            {
            	// atomicAcquireUnowned already set ownership of the persisted lease, just update the live lease.
                leaseToAcquire.setOwner(this.host.getHostName());
            	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, leaseToAcquire,
                        "acquireLease() acquired lease"));
            	leaseInStore = wasUnowned;
            	leaseToAcquire.setExpirationTime(leaseInStore.getExpirationTime());
            }
            else
            {
	            if (leaseInStore.getOwner().compareTo(this.host.getHostName()) == 0)
	            {
	            	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, leaseToAcquire,
                            "acquireLease() already hold lease"));
	            }
	            else
	            {
	            	String oldOwner = leaseInStore.getOwner();
	            	// Make change in both persisted lease and live lease!
	            	InMemoryLeaseStore.singleton.stealLease(leaseInStore, this.host.getHostName());
	            	leaseToAcquire.setOwner(this.host.getHostName());
	            	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, leaseToAcquire,
                            "acquireLease() stole lease from " + oldOwner));
	            }
	            long newExpiration = System.currentTimeMillis() + getLeaseDurationInMilliseconds();
	        	// Make change in both persisted lease and live lease!
	            leaseInStore.setExpirationTime(newExpiration);
	            leaseToAcquire.setExpirationTime(newExpiration);
            }
        }
        else
        {
        	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, leaseToAcquire,
                    "acquireLease() can't find lease"));
        	retval = false;
        }
        
        return retval;
    }
    
    // Real partition pumps get "notified" when another host has stolen their lease because the receiver throws
    // a ReceiverDisconnectedException. It doesn't matter how many hosts try to steal the lease at the same time,
    // only one will end up with it and that one will kick the others off via the exclusivity of epoch receivers.
    // This mechanism simulates that for dummy partition pumps used in testing. If expectedOwner does not currently
    // own the lease for the given partition, then notifier is called immediately, otherwise it is called whenever
    // ownership of the lease changes.
    public void notifyOnSteal(String expectedOwner, String partitionId, Callable<?> notifier)
    {
    	InMemoryLeaseStore.singleton.notifyOnSteal(expectedOwner, partitionId, notifier);
    }
    
    @Override
    public boolean renewLease(Lease lease)
    {
    	InMemoryLease leaseToRenew = (InMemoryLease)lease;
    	
    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, leaseToRenew, "renewLease()"));
    	
    	boolean retval = true;
    	InMemoryLease leaseInStore = InMemoryLeaseStore.singleton.getLease(leaseToRenew.getPartitionId());
        if (leaseInStore != null)
        {
        	// CHANGE TO MATCH BEHAVIOR OF AzureStorageCheckpointLeaseManager
        	// Renewing a lease that has expired succeeds unless some other host has grabbed it already.
        	// So don't check expiration, just ownership.
        	if (/* !wrapIsExpired(leaseInStore) && */ (leaseInStore.getOwner().compareTo(this.host.getHostName()) == 0))
        	{
                long newExpiration = System.currentTimeMillis() + getLeaseDurationInMilliseconds();
            	// Make change in both persisted lease and live lease!
                leaseInStore.setExpirationTime(newExpiration);
                leaseToRenew.setExpirationTime(newExpiration);
        	}
        	else
            {
            	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, leaseToRenew,
                        "renewLease() not renewed because we don't own lease"));
            	retval = false;
            }
        }
        else
        {
        	TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, leaseToRenew,
                    "renewLease() can't find lease"));
        	retval = false;
        }
        
        return retval;
    }

    @Override
    public boolean releaseLease(Lease lease)
    {
    	InMemoryLease leaseToRelease = (InMemoryLease)lease;
    	
    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, leaseToRelease, "releaseLease()"));
    	
    	boolean retval = true;
    	InMemoryLease leaseInStore = InMemoryLeaseStore.singleton.getLease(leaseToRelease.getPartitionId());
    	if (leaseInStore != null)
    	{
    		if (!leaseInStore.isExpired() && (leaseInStore.getOwner().compareTo(this.host.getHostName()) == 0))
    		{
	    		TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, leaseToRelease, "releaseLease() released OK"));
	        	// Make change in both persisted lease and live lease!
	    		leaseInStore.setOwner("");
	    		leaseToRelease.setOwner("");
	    		leaseInStore.setExpirationTime(0);
	    		leaseToRelease.setExpirationTime(0);
    		}
    		else
    		{
	    		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, leaseToRelease,
                        "releaseLease() not released because we don't own lease"));
    			retval = false;
    		}
    	}
    	else
    	{
    		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, leaseToRelease, "releaseLease() can't find lease"));
    		retval = false;
    	}
    	return retval;
    }

    @Override
    public boolean updateLease(Lease lease)
    {
    	InMemoryLease leaseToUpdate = (InMemoryLease)lease;
    	
    	TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, leaseToUpdate, "updateLease()"));
    	
    	// Renew lease first so it doesn't expire in the middle.
    	boolean retval = renewLease(leaseToUpdate);
    	
    	if (retval)
    	{
	    	InMemoryLease leaseInStore = InMemoryLeaseStore.singleton.getLease(leaseToUpdate.getPartitionId());
	    	if (leaseInStore != null)
	    	{
	    		if (!leaseInStore.isExpired() && (leaseInStore.getOwner().compareTo(this.host.getHostName()) == 0))
	    		{
	    			// We are updating with values already in the live lease, so only need to set on the persisted lease.
	   				leaseInStore.setEpoch(leaseToUpdate.getEpoch());
	    			leaseInStore.setToken(leaseToUpdate.getToken());
	    			// Don't copy expiration time, that is managed directly by Acquire/Renew/Release
	    		}
	    		else
	    		{
		    		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, leaseToUpdate,
                            "updateLease() not updated because we don't own lease"));
	    			retval = false;
	    		}
	    	}
	    	else
	    	{
	    		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, leaseToUpdate,
                        "updateLease() can't find lease"));
	    		retval = false;
	    	}
    	}
    	
    	return retval;
    }
    

    private static class InMemoryLeaseStore
    {
        final static InMemoryLeaseStore singleton = new InMemoryLeaseStore();
        private static int leaseDurationInMilliseconds;

        private ConcurrentHashMap<String, InMemoryLease> inMemoryLeasesPrivate = null;
        private ConcurrentHashMap<String, Callable<?>> notifiers = new ConcurrentHashMap<String, Callable<?>>();
        
        synchronized boolean existsMap()
        {
        	return (this.inMemoryLeasesPrivate != null);
        }
        
        synchronized void initializeMap(int leaseDurationInMilliseconds)
        {
        	if (this.inMemoryLeasesPrivate == null)
        	{
        		this.inMemoryLeasesPrivate = new ConcurrentHashMap<String, InMemoryLease>();
        	}
        	InMemoryLeaseStore.leaseDurationInMilliseconds = leaseDurationInMilliseconds;
        }
        
        synchronized void deleteMap()
        {
        	this.inMemoryLeasesPrivate = null;
        }
        
        synchronized InMemoryLease getLease(String partitionId)
        {
        	return this.inMemoryLeasesPrivate.get(partitionId);
        }
        
        synchronized InMemoryLease atomicAquireUnowned(String partitionId, String newOwner)
        {
        	InMemoryLease leaseInStore = getLease(partitionId);
			if (leaseInStore.isExpired() || (leaseInStore.getOwner() == null) || leaseInStore.getOwner().isEmpty())
			{
				leaseInStore.setOwner(newOwner);
                leaseInStore.setExpirationTime(System.currentTimeMillis() + InMemoryLeaseStore.leaseDurationInMilliseconds);
			}
			else
			{
				// Return null if it was already owned
				leaseInStore = null;
			}
        	return leaseInStore;
        }
        
        synchronized void notifyOnSteal(String expectedOwner, String partitionId, Callable<?> notifier)
        {
        	InMemoryLease leaseInStore = getLease(partitionId);
        	if ((leaseInStore.getOwner() != null) && (expectedOwner.compareTo(leaseInStore.getOwner()) != 0))
        	{
        		// Already stolen.
        		try
        		{
					notifier.call();
				}
        		catch (Exception e)
        		{
				}
        	}
        	else
        	{
        		this.notifiers.put(partitionId, notifier);
        	}
        }
        
        synchronized void stealLease(InMemoryLease stealee, String newOwner)
        {
        	stealee.setOwner(newOwner);
        	Callable<?> notifier = this.notifiers.get(stealee.getPartitionId());
        	if (notifier != null)
        	{
        		try
        		{
					notifier.call();
				}
        		catch (Exception e)
        		{
				}
        	}
        }
        
        synchronized void setOrReplaceLease(InMemoryLease newLease)
        {
        	this.inMemoryLeasesPrivate.put(newLease.getPartitionId(), newLease);
        }
        
        synchronized void removeLease(InMemoryLease goneLease)
        {
        	this.inMemoryLeasesPrivate.remove(goneLease.getPartitionId());
        }
    }
    
    
    private static class InMemoryLease extends Lease
    {
    	private long expirationTimeMillis = 0;

        private final static Logger TRACE_LOGGER = LoggerFactory.getLogger(InMemoryLease.class);
    	
		InMemoryLease(String partitionId)
		{
			super(partitionId);
		}
		
		InMemoryLease(InMemoryLease source)
		{
			super(source);
			this.expirationTimeMillis = source.expirationTimeMillis;
		}
		
		void setExpirationTime(long expireAtMillis)
		{
			this.expirationTimeMillis = expireAtMillis;
		}
		
		long getExpirationTime()
		{
			return this.expirationTimeMillis;
		}
		
		@Override
	    public boolean isExpired()
	    {
			boolean hasExpired = (System.currentTimeMillis() >= this.expirationTimeMillis);
			if (hasExpired)
			{
	        	// CHANGE TO MATCH BEHAVIOR OF AzureStorageCheckpointLeaseManager
				// An expired lease can be renewed by the previous owner. In order to implement that behavior for
				// InMemory, the owner field has to remain unchanged.
				//setOwner("");
			}
			TRACE_LOGGER.info("isExpired(" + this.getPartitionId() + (hasExpired? ") expired " : ") leased ") + (this.expirationTimeMillis - System.currentTimeMillis()));
			return hasExpired;
	    }
    }
}
