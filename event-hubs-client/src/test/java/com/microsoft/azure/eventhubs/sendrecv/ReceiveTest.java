package com.microsoft.azure.eventhubs.sendrecv;

import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

import org.junit.*;

import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.eventhubs.lib.*;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.amqp.*;

public class ReceiveTest extends TestBase
{
	@Test()
	public void testReceiverFilters() throws ServiceBusException, InterruptedException, ExecutionException, IOException
	{
		Assume.assumeTrue(TestBase.isServiceRun());
		
		TestEventHubInfo eventHubInfo = TestBase.checkoutTestEventHub();
		EventHubClient ehClient = null;
		PartitionReceiver offsetReceiver = null;
		PartitionReceiver datetimeReceiver = null;
		
		try 
		{
			ConnectionStringBuilder connectionString = TestBase.getConnectionString(eventHubInfo);
			ehClient = EventHubClient.createFromConnectionString(connectionString.toString()).get();
			
			String cgName = eventHubInfo.getRandomConsumerGroup();
			String partitionId = "0";
			
			TestBase.pushEventsToPartition(ehClient, partitionId, 10).get();
			
			offsetReceiver = ehClient.createReceiver(cgName, partitionId, PartitionReceiver.StartOfStream).get();
			Iterable<EventData> startingEventsUsingOffsetReceiver = offsetReceiver.receive().get();
			
			Assert.assertTrue(startingEventsUsingOffsetReceiver != null && startingEventsUsingOffsetReceiver.iterator().hasNext());
			
			// Test1: Validate DateTimeReceiver returns correct startingOffset with startOfEpoch
			datetimeReceiver = ehClient.createReceiver(cgName, partitionId, Instant.EPOCH).get();
			Iterable<EventData> startingEventsUsingDateTimeReceiver = datetimeReceiver.receive().get();
			
			Assert.assertTrue(startingEventsUsingOffsetReceiver != null && startingEventsUsingDateTimeReceiver.iterator().hasNext());
			
			int counter = 0;
			Iterator<EventData> dateTimeIterator = startingEventsUsingDateTimeReceiver.iterator();
			for(EventData eventDataUsingOffset: startingEventsUsingOffsetReceiver)
			{
				EventData eventDataUsingDateTime = dateTimeIterator.next();
				System.out.println(String.format("recv by offset: %s.", eventDataUsingOffset.getSystemProperties().getOffset()));
				System.out.println(String.format("recv by dateTime: %s.", eventDataUsingDateTime.getSystemProperties().getOffset()));
				
				Assert.assertTrue(eventDataUsingOffset.getSystemProperties().getOffset().equalsIgnoreCase(eventDataUsingDateTime.getSystemProperties().getOffset()));
				
				counter++;
				if (!dateTimeIterator.hasNext())
					break;
			}
			
			datetimeReceiver.close();
			
			Iterator<EventData> offsetIterator = startingEventsUsingOffsetReceiver.iterator();
			offsetIterator.next();
			// Test2: pick a random event from OffsetReceiver and then validate DateTime receiver using SystemProperties
			if (!offsetIterator.hasNext()) {
				startingEventsUsingOffsetReceiver = offsetReceiver.receive().get();
			}
			
			Assert.assertTrue(startingEventsUsingOffsetReceiver.iterator().hasNext());
			EventData nextEvent = startingEventsUsingOffsetReceiver.iterator().next();
			datetimeReceiver = ehClient.createReceiver(cgName, partitionId, 
				nextEvent.getSystemProperties().getEnqueuedTime().minusMillis(1)).get();
			
			Iterable<EventData> dateTimeEventsFromCustomOffset = datetimeReceiver.receive().get();
			Assert.assertTrue(dateTimeEventsFromCustomOffset.iterator().hasNext());
			EventData firstEventAfterGivenTime = dateTimeEventsFromCustomOffset.iterator().next();
			System.out.println(firstEventAfterGivenTime.getSystemProperties().getEnqueuedTime());
			Assert.assertTrue(firstEventAfterGivenTime.getSystemProperties().getOffset().
					equals(nextEvent.getSystemProperties().getOffset()));
		}
		finally
		{
			TestBase.checkinTestEventHub(eventHubInfo.getName());
			if (offsetReceiver != null) {
				offsetReceiver.close();
			}
			if (datetimeReceiver != null) {
				datetimeReceiver.close();
			}
			
			if (ehClient != null) {
				ehClient.close();
			}
		}
	}

}
