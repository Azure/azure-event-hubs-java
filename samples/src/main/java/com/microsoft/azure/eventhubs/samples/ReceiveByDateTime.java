package com.microsoft.azure.eventhubs.samples;

import java.io.IOException;
import java.time.*;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.function.*;
import java.util.logging.*;

import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.servicebus.*;

public class ReceiveByDateTime
{

	public static void main(String[] args) 
			throws ServiceBusException, ExecutionException, InterruptedException, IOException
	{
		ConnectionStringBuilder connStr = new ConnectionStringBuilder("----namespaceName-----", "----EventHubName-----", "-----sayKeyName-----", "---SasKey----");
		
		EventHubClient ehClient = EventHubClient.createFromConnectionString(connStr.toString()).get();
		
		// receiver
		String partitionId = "0"; // API to get PartitionIds will be released as part of V0.2
		
		PartitionReceiver receiver = ehClient.createEpochReceiver(
				EventHubClient.DefaultConsumerGroupName, 
				partitionId, 
				Instant.now().minus(Duration.ofDays(5)),
				2345).get();
		
		System.out.println("date-time receiver created...");
		
		while (true)
		{
			receiver.receive().thenAccept(new Consumer<Collection<EventData>>()
			{
				public void accept(Collection<EventData> receivedEvents)
				{
					System.out.println(String.format("ReceivedBatch Size: %s", receivedEvents != null ? receivedEvents.size() : 0));
					
					for(EventData receivedEvent: receivedEvents)
					{
						System.out.println(String.format("Offset: %s, SeqNo: %s, EnqueueTime: %s", 
								receivedEvent.getSystemProperties().getOffset(), 
								receivedEvent.getSystemProperties().getSequenceNumber(), 
								receivedEvent.getSystemProperties().getEnqueuedTime()));
					}
				}
			}).get();
		}
	}

	/**
	 * actual application-payload, ex: a telemetry event
	 */
	static final class PayloadEvent
	{
		PayloadEvent()	{}
		
		public String strProperty;
		public long longProperty;
		public int intProperty;
	}

}
