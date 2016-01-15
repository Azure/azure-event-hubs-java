package com.microsoft.azure.eventhubs;

import java.util.*;
import java.util.function.Consumer;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;

import com.microsoft.azure.servicebus.MessageSender;
import com.microsoft.azure.servicebus.amqp.AmqpConstants;

/*
 * Internal utility class for EventData
 */
final class EventDataUtil
{
	private EventDataUtil(){}
	
	static LinkedList<EventData> toEventDataCollection(final Collection<Message> messages)
	{
		if (messages == null)
		{
			return null;
		}
		
		// TODO: no-copy solution
		LinkedList<EventData> events = new LinkedList<EventData>();
		for(Message message : messages)
		{
			events.add(new EventData(message));
		}
		
		return events;
	}
	
	static Iterable<Message> toAmqpMessages(final Iterable<EventData> eventDatas, final String partitionKey)
	{
		final LinkedList<Message> messages = new LinkedList<Message>();
		eventDatas.forEach(new Consumer<EventData>()
		{
			@Override
			public void accept(EventData eventData)
			{				
				Message amqpMessage = partitionKey == null ? eventData.toAmqpMessage() : eventData.toAmqpMessage(partitionKey);
				messages.add(amqpMessage);
			}
		});
		
		return messages;
	}
	
	static Iterable<Message> toAmqpMessages(final Iterable<EventData> eventDatas)
	{
		return EventDataUtil.toAmqpMessages(eventDatas, null);
	}
}
