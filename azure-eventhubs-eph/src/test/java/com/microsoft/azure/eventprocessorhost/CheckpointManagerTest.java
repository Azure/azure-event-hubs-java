/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.microsoft.azure.eventhubs.EventHubClient;

import static org.junit.Assert.*;

public class CheckpointManagerTest
{
	private String azureStorageConnectionString = TestUtilities.getStorageConnectionString();
	
	private ILeaseManager[] leaseManagers;
	private ICheckpointManager[] checkpointManagers;
	private EventProcessorHost[] hosts;
	
	@Test
	public void singleManangerInMemoryCheckpointSmokeTest() throws Exception
	{
		singleManagerCheckpointSmokeTest(false, 8);
	}
	
	@Test
	public void twoManagerInMemoryCheckpointSmokeTest() throws Exception
	{
		twoManagerCheckpointSmokeTest(false, 8);
	}
	
	@Test
	public void singleManagerAzureCheckpointSmokeTest() throws Exception
	{
		RealEventHubUtilities rUtils = new RealEventHubUtilities();
		singleManagerCheckpointSmokeTest(true, rUtils.getPartitionIdsForTest().size());
	}
	
	@Test
	public void twoManagerAzureCheckpointSmokeTest() throws Exception
	{
		RealEventHubUtilities rUtils = new RealEventHubUtilities();
		twoManagerCheckpointSmokeTest(true, rUtils.getPartitionIdsForTest().size());
	}
	
	public void singleManagerCheckpointSmokeTest(boolean useAzureStorage, int partitionCount) throws Exception
	{
		this.leaseManagers = new ILeaseManager[1];
		this.checkpointManagers = new ICheckpointManager[1];
		this.hosts = new EventProcessorHost[1];
		setupOneManager(useAzureStorage, 0, "0", generateContainerName("0"));
		
		TestUtilities.log("singleManagerCheckpointSmokeTest");
		TestUtilities.log("USING " + (useAzureStorage ? "AzureStorageCheckpointLeaseManager" : "InMemoryCheckpointManager"));

		TestUtilities.log("Check whether checkpoint store exists before create");
		boolean boolret = this.checkpointManagers[0].checkpointStoreExists().get();
		assertFalse("checkpoint store should not exist yet", boolret);
		
		TestUtilities.log("Create checkpoint store");
		this.checkpointManagers[0].createCheckpointStoreIfNotExists().get();

		TestUtilities.log("Check whether checkpoint store exists after create");
		boolret = this.checkpointManagers[0].checkpointStoreExists().get();
		assertTrue("checkpoint store should exist but does not", boolret);
		
		TestUtilities.log("Create checkpoint holders for all partitions");
		Checkpoint[] checkpoints = new Checkpoint[partitionCount];
		for (int i = 0; i < partitionCount; i++)
		{
			Checkpoint createdCheckpoint = this.checkpointManagers[0].createCheckpointIfNotExists(String.valueOf(i)).get();
			checkpoints[i] = createdCheckpoint;
			assertNull("unexpected already existing checkpoint for " + i, createdCheckpoint);
		}

		TestUtilities.log("Trying to get checkpoints for all partitions");
		for (int i = 0; i < partitionCount; i++)
		{
			Checkpoint blah = this.checkpointManagers[0].getCheckpoint(String.valueOf(i)).get();
			assertNull("unexpectedly successful retrieve checkpoint for " + i, blah);
		}
		
		// AzureStorageCheckpointLeaseManager tries to pretend that checkpoints and leases are separate, but they really aren't.
		// Because the checkpoint data is stored in the lease, updating the checkpoint means updating the lease, and it is
		// necessary to hold the lease in order to update it.
		HashMap<String, Lease> leases = new HashMap<String, Lease>();
		if (useAzureStorage)
		{
			List<Lease> leaseList = this.leaseManagers[0].getAllLeases().get();
			assertNotNull("failed to retrieve leases", leaseList);
			for (Lease l : leaseList)
			{
				leases.put(l.getPartitionId(), l);
				boolret = this.leaseManagers[0].acquireLease(l).get();
				assertTrue("failed to acquire lease for " + l.getPartitionId(), boolret);
			}
		}
		
		TestUtilities.log("Creating checkpoints for all partitions");
		for (int i = 0; i < partitionCount; i++)
		{
			// Arbitrary values, just checking that they are persisted
			checkpoints[i] = new Checkpoint(String.valueOf(i));
			checkpoints[i].setOffset(String.valueOf(i * 234));
			checkpoints[i].setSequenceNumber(i + 77);
			this.checkpointManagers[0].updateCheckpoint(leases.get(String.valueOf(i)), checkpoints[i]).get();
		}
		
		TestUtilities.log("Getting checkpoints for all partitions and verifying");
		for (int i = 0; i < partitionCount; i++)
		{
			Checkpoint blah = this.checkpointManagers[0].getCheckpoint(String.valueOf(i)).get();
			assertNotNull("failed to retrieve checkpoint for " + i, blah);
			assertEquals("retrieved offset does not match written offset", blah.getOffset(), checkpoints[i].getOffset());
			assertEquals("retrieved seqno does not match written seqno", blah.getSequenceNumber(), checkpoints[i].getSequenceNumber());
		}

		// Have to release the leases before we can delete the store.
		if (useAzureStorage)
		{
			List<Lease> leaseList = this.leaseManagers[0].getAllLeases().get();
			assertNotNull("failed to retrieve leases", leaseList);
			for (Lease l : leaseList)
			{
				this.leaseManagers[0].releaseLease(l).get();
			}
		}
		
		TestUtilities.log("Cleaning up checkpoint store");
		boolret = this.checkpointManagers[0].deleteCheckpointStore().get();
		assertTrue("failed while cleaning up store", boolret);
		
		TestUtilities.log("singleManagerCheckpointSmokeTest DONE");
	}
	
	public void twoManagerCheckpointSmokeTest(boolean useAzureStorage, int partitionCount) throws Exception
	{
		this.leaseManagers = new ILeaseManager[2];
		this.checkpointManagers = new ICheckpointManager[2];
		this.hosts = new EventProcessorHost[2];
		String containerName = generateContainerName(null);
		setupOneManager(useAzureStorage, 0, "twoCheckpoint", containerName);
		setupOneManager(useAzureStorage, 1, "twoCheckpoint", containerName);
		
		TestUtilities.log("twoManagerCheckpointSmokeTest");
		TestUtilities.log("USING " + (useAzureStorage ? "AzureStorageCheckpointLeaseManager" : "InMemoryLeaseManager"));
		
		TestUtilities.log("Check whether checkpoint store exists before create");
		boolean boolret = this.checkpointManagers[0].checkpointStoreExists().get();
		assertFalse("checkpoint store should not exist yet", boolret);
		
		TestUtilities.log("Second manager create checkpoint store");
		this.checkpointManagers[1].createCheckpointStoreIfNotExists().get();

		TestUtilities.log("First mananger check whether checkpoint store exists after create");
		boolret = this.checkpointManagers[0].checkpointStoreExists().get();
		assertTrue("checkpoint store should exist but does not", boolret);
		
		TestUtilities.log("Alternately create checkpoint holders for all partitions");
		Checkpoint[] checkpoints = new Checkpoint[partitionCount];
		for (int i = 0; i < partitionCount; i++)
		{
			Checkpoint createdCheckpoint = this.checkpointManagers[i % 2].createCheckpointIfNotExists(String.valueOf(i)).get();
			checkpoints[i] = createdCheckpoint;
			assertNull("unexpected already existing checkpoint for " + i, createdCheckpoint);
		}

		TestUtilities.log("Try to get each others checkpoints for all partitions");
		for (int i = 0; i < partitionCount; i++)
		{
			Checkpoint blah = this.checkpointManagers[(i + 1) % 2].getCheckpoint(String.valueOf(i)).get();
			assertNull("unexpected successful retrieve checkpoint for " + i, blah);
		}
		
		// AzureStorageCheckpointLeaseManager tries to pretend that checkpoints and leases are separate, but they really aren't.
		// Because the checkpoint data is stored in the lease, updating the checkpoint means updating the lease, and it is
		// necessary to hold the lease in order to update it.
		HashMap<String, Lease> leases = new HashMap<String, Lease>();
		if (useAzureStorage)
		{
			List<Lease> leaseList = this.leaseManagers[0].getAllLeases().get();
			assertNotNull("failed to retrieve leases", leaseList);
			for (Lease l : leaseList)
			{
				leases.put(l.getPartitionId(), l);
				boolret = this.leaseManagers[0].acquireLease(l).get();
				assertTrue("failed to acquire lease for " + l.getPartitionId(), boolret);
			}
		}

		TestUtilities.log("Second manager update checkpoints for all partitions");
		for (int i = 0; i < partitionCount; i++)
		{
			// Arbitrary values, just checking that they are persisted
			checkpoints[i] = new Checkpoint(String.valueOf(i));
			checkpoints[i].setOffset(String.valueOf(i * 234));
			checkpoints[i].setSequenceNumber(i + 77);
			this.checkpointManagers[1].updateCheckpoint(leases.get(String.valueOf(i)), checkpoints[i]).get();
		}
		
		TestUtilities.log("First manager get and verify checkpoints for all partitions");
		for (int i = 0; i < partitionCount; i++)
		{
			Checkpoint blah = this.checkpointManagers[0].getCheckpoint(String.valueOf(i)).get();
			assertNotNull("failed to retrieve checkpoint for " + i, blah);
			assertEquals("retrieved offset does not match written offset", blah.getOffset(), checkpoints[i].getOffset());
			assertEquals("retrieved seqno does not match written seqno", blah.getSequenceNumber(), checkpoints[i].getSequenceNumber());
		}

		// Have to release the leases before we can delete the store.
		if (useAzureStorage)
		{
			List<Lease> leaseList = this.leaseManagers[0].getAllLeases().get();
			assertNotNull("failed to retrieve leases", leaseList);
			for (Lease l : leaseList)
			{
				assertNotNull("failed to retrieve lease", l);
				this.leaseManagers[0].releaseLease(l).get();
			}
		}
		
		TestUtilities.log("Clean up checkpoint store");
		boolret = this.checkpointManagers[0].deleteCheckpointStore().get();
		assertTrue("failed while cleaning up store", boolret);
		
		TestUtilities.log("twoManagerCheckpointSmokeTest DONE");
	}

	private String generateContainerName(String infix)
	{
		StringBuilder containerName = new StringBuilder(64);
		containerName.append("ckptmgrtest-");
		if (infix != null)
		{
			containerName.append(infix);
			containerName.append('-');
		}
		containerName.append(UUID.randomUUID().toString());
		return containerName.toString();
	}
	
	private void setupOneManager(boolean useAzureStorage, int index, String suffix, String containerName) throws Exception
	{
		ILeaseManager leaseMgr = null;
		ICheckpointManager checkpointMgr = null;
		
		if (!useAzureStorage)
		{
			leaseMgr = new InMemoryLeaseManager();
			checkpointMgr = new InMemoryCheckpointManager();
		}
		else
		{
			TestUtilities.log("Container name: " + containerName);
			AzureStorageCheckpointLeaseManager azMgr = new AzureStorageCheckpointLeaseManager(this.azureStorageConnectionString, containerName);
			leaseMgr = azMgr;
			checkpointMgr = azMgr;
		}
		
		// Host name needs to be unique per host so use index. Event hub should be the same for all hosts in a test, so use the supplied suffix.
    	EventProcessorHost host = new EventProcessorHost("dummyHost" + String.valueOf(index), "NOTREAL" + suffix,
    			EventHubClient.DEFAULT_CONSUMER_GROUP_NAME, TestUtilities.syntacticallyCorrectDummyConnectionString + suffix, checkpointMgr, leaseMgr);
    	
    	
    	try
    	{
    		if (!useAzureStorage)
    		{
    			((InMemoryLeaseManager)leaseMgr).initialize(host.getHostContext());
    			((InMemoryCheckpointManager)checkpointMgr).initialize(host.getHostContext());
    		}
    		else
    		{
    			((AzureStorageCheckpointLeaseManager)checkpointMgr).initialize(host.getHostContext());
    		}
		}
    	catch (Exception e)
    	{
    		TestUtilities.log("Manager initializion failed");
    		throw e;
		}
		
    	this.leaseManagers[index] = leaseMgr;
    	this.checkpointManagers[index] = checkpointMgr;
    	this.hosts[index] = host;
	}
}
