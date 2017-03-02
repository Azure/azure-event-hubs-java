/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.eventdata;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.LinkedList;
import java.util.List;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.message.Message;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.IllegalEventDataBodyException;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.MessageSender;
import com.microsoft.azure.servicebus.MessagingFactory;
import com.microsoft.azure.servicebus.ServiceBusException;
import com.microsoft.azure.servicebus.amqp.AmqpConstants;

public class InteropEventBodyTest extends ApiTestBase {
    
    static EventHubClient ehClient;
    static MessagingFactory msgFactory;
    static PartitionReceiver receiver;
    static MessageSender partitionMsgSender;

    static final String partitionId = "0";
    static EventData receivedEvent;
    static EventData reSentAndReceivedEvent;
    static Message reSendAndReceivedMessage;

    @BeforeClass
    public static void initialize() throws ServiceBusException, IOException, InterruptedException, ExecutionException
    {
        final ConnectionStringBuilder connStrBuilder = TestContext.getConnectionString();
        final String connectionString = connStrBuilder.toString();

        ehClient = EventHubClient.createFromConnectionStringSync(connectionString);
        msgFactory = MessagingFactory.createFromConnectionString(connectionString).get();
        receiver = ehClient.createReceiverSync(TestContext.getConsumerGroupName(), partitionId, Instant.now());
        partitionMsgSender = MessageSender.create(msgFactory, "link1", connStrBuilder.getEntityPath() + "/partitions/" + partitionId).get();
        
        // run out of messages in that specific partition - to account for clock-skew with Instant.now() on test machine vs eventhubs service
        receiver.setReceiveTimeout(Duration.ofSeconds(5));
        Iterable<EventData> clockSkewEvents;
        do {
            clockSkewEvents = receiver.receiveSync(100);
        } while (clockSkewEvents != null && clockSkewEvents.iterator().hasNext());
    }

    @Test
    public void interopWithProtonAmqpMessageBodyAsAmqpValue() throws ServiceBusException, InterruptedException, ExecutionException
    {
        Message originalMessage = Proton.message();
        String payload = "testmsg";
        originalMessage.setBody(new AmqpValue(payload));
        partitionMsgSender.send(originalMessage).get();
        receivedEvent = receiver.receiveSync(10).iterator().next();
        
        Assert.assertTrue(receivedEvent.getSystemProperties().get(AmqpConstants.AMQP_VALUE).equals(payload));
        
        try {
            receivedEvent.getBody();
            Assert.assertTrue(false);
        } catch (IllegalEventDataBodyException exception) {
            Assert.assertTrue(exception.getSystemPropertyName().equals(AmqpConstants.AMQP_VALUE));
        }
    }
    
    @Test
    public void interopWithProtonAmqpMessageBodyAsAmqpSequence() throws ServiceBusException, InterruptedException, ExecutionException
    {
        Message originalMessage = Proton.message();
        String payload = "testmsg";
        LinkedList<Data> datas = new LinkedList<>();
        datas.add(new Data(new Binary(payload.getBytes())));
        originalMessage.setBody(new AmqpSequence(datas));
        
        partitionMsgSender.send(originalMessage).get();
        receivedEvent = receiver.receiveSync(10).iterator().next();
        
        Assert.assertTrue(new String(((List<Data>)(receivedEvent.getSystemProperties().get(AmqpConstants.AMQP_SEQUENCE))).get(0).getValue().getArray()).equals(payload));
    
        try {
            receivedEvent.getBody();
            Assert.assertTrue(false);
        } catch (IllegalEventDataBodyException exception) {
            Assert.assertTrue(exception.getSystemPropertyName().equals(AmqpConstants.AMQP_SEQUENCE));
        }
    }
    
    @AfterClass
    public static void cleanup() throws ServiceBusException
    {
        if (partitionMsgSender != null)
                partitionMsgSender.closeSync();

        if (receiver != null)
                receiver.closeSync();

        if (ehClient != null)
                ehClient.closeSync();

        if (msgFactory != null)
                msgFactory.closeSync();
    }
}
