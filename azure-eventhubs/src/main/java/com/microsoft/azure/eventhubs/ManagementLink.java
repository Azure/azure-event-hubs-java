/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.message.Message;

import com.microsoft.azure.servicebus.ClientConstants;
import com.microsoft.azure.servicebus.ClientEntity;
import com.microsoft.azure.servicebus.MessagingFactory;
import com.microsoft.azure.servicebus.RequestResponseLink;
import com.microsoft.azure.servicebus.amqp.AmqpException;
import com.microsoft.azure.servicebus.amqp.AmqpManagementResponseCode;
import java.time.Instant;
import java.util.Date;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;

public class ManagementLink extends ClientEntity
{
    private final MessagingFactory factory;
    
    private RequestResponseLink requestResponseLink;
    
    private ManagementLink(MessagingFactory factory, String clientId)
    {
        super(clientId, factory);
        
        this.factory = factory;
    }
    
    public static CompletableFuture<ManagementLink> create(final MessagingFactory factory, final String clientId)
    {
        ManagementLink managementLink = new ManagementLink(factory, clientId);
        return RequestResponseLink.create(factory, clientId + ":management", ClientConstants.MANAGEMENT_ADDRESS)
            .thenComposeAsync(new Function<RequestResponseLink, CompletableFuture<ManagementLink>>()
                {
                    @Override
                    public CompletableFuture<ManagementLink> apply(RequestResponseLink requestReponseLink)
                    {
                        managementLink.requestResponseLink = requestReponseLink;
                        return CompletableFuture.completedFuture(managementLink);
                    }
                });
    }
    
    public CompletableFuture<EventHubRuntimeInformation> getEventHubRuntimeInformation(final String eventHubName)
    {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ClientConstants.MANAGEMENT_ENTITY_TYPE_KEY, ClientConstants.MANAGEMENT_EVENTHUB_ENTITY_TYPE);
        properties.put(ClientConstants.MANAGEMENT_ENTITY_NAME_KEY, eventHubName);
        
        return this.requestRead(properties)
            .thenComposeAsync(new Function<Map<String, Object>, CompletableFuture<EventHubRuntimeInformation>>()
            {
                @Override
                public CompletableFuture<EventHubRuntimeInformation> apply(Map<String, Object> resultMap)
                {                    
                    EventHubRuntimeInformation runtimeInformation = new EventHubRuntimeInformation(
                        (String) resultMap.get(ClientConstants.MANAGEMENT_ENTITY_NAME_KEY),
                        (int) resultMap.get(ClientConstants.MANAGEMENT_RESULT_PARTITION_COUNT),
                        (String[]) resultMap.get(ClientConstants.MANAGEMENT_RESULT_PARTITION_IDS));

                    return CompletableFuture.completedFuture(runtimeInformation);
                }
            });
    }
    
    public CompletableFuture<PartitionRuntimeInformation> getPartitionRuntimeInformation(final String eventHubName, final String partitionId)
    {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ClientConstants.MANAGEMENT_ENTITY_TYPE_KEY, ClientConstants.MANAGEMENT_PARTITION_ENTITY_TYPE);
        properties.put(ClientConstants.MANAGEMENT_ENTITY_NAME_KEY, eventHubName);
        properties.put(ClientConstants.MANAGEMENT_PARTITION_NAME_KEY, partitionId);
        
        return this.requestRead(properties)
            .thenComposeAsync(new Function<Map<String, Object>, CompletableFuture<PartitionRuntimeInformation>>()
            {
                @Override
                public CompletableFuture<PartitionRuntimeInformation> apply(Map<String, Object> resultMap)
                {
                    PartitionRuntimeInformation runtimeInformation = new PartitionRuntimeInformation(
                        (String) resultMap.get(ClientConstants.MANAGEMENT_ENTITY_NAME_KEY),
                        (String) resultMap.get(ClientConstants.MANAGEMENT_PARTITION_NAME_KEY),
                        (long) resultMap.get(ClientConstants.MANAGEMENT_RESULT_BEGIN_SEQUENCE_NUMBER),
                        (long) resultMap.get(ClientConstants.MANAGEMENT_RESULT_LAST_ENQUEUED_SEQUENCE_NUMBER),
                        (String) resultMap.get(ClientConstants.MANAGEMENT_RESULT_LAST_ENQUEUED_OFFSET),
                        ((Date) resultMap.get(ClientConstants.MANAGEMENT_RESULT_LAST_ENQUEUED_TIME_UTC)).toInstant());

                    return CompletableFuture.completedFuture(runtimeInformation);
                }
            });
    }
    
    private CompletableFuture<Map<String, Object>> requestRead(Map<String, String> requestProperties)
    {
        final Message request = Proton.message();
        final Map<String, String> properties = new HashMap<>();
        properties.put(ClientConstants.MANAGEMENT_OPERATION_KEY, ClientConstants.READ_OPERATION_VALUE);
        properties.putAll(requestProperties);
        final ApplicationProperties applicationProperties = new ApplicationProperties(properties);
        
        request.setApplicationProperties(applicationProperties);
        
        return this.requestResponseLink.request(request)
            .thenComposeAsync(new Function<Message, CompletableFuture<Map<String, Object>>>()
            {
                @Override
                public CompletableFuture<Map<String, Object>> apply(Message response)
                {
                    final int statusCode = (int) response.getApplicationProperties().getValue().get(ClientConstants.MANAGEMENT_STATUS_CODE_KEY);
                    final String statusDescription = (String) response.getApplicationProperties().getValue().get(ClientConstants.MANAGEMENT_STATUS_DESCRIPTION_KEY);
                    final CompletableFuture<Map<String, Object>> result = new CompletableFuture<>();
                    
                    if (statusCode == AmqpManagementResponseCode.ACCEPTED.getValue() || statusCode == AmqpManagementResponseCode.OK.getValue())
                    {
                        if (response.getBody() == null)
                            result.complete(null);
                        else
                            result.complete((Map<String, Object>) ((AmqpValue) response.getBody()).getValue());
                    }
                    else
                    {
                        final Symbol condition = (Symbol) response.getApplicationProperties().getValue().get(ClientConstants.MANAGEMENT_RESPONSE_ERROR_CONDITION);
                        final ErrorCondition error = new ErrorCondition(condition, statusDescription);
                        result.completeExceptionally(new AmqpException(error));
                    }

                    return result;
                }
            });
    }

    @Override
    protected CompletableFuture<Void> onClose()
    {
        return this.requestResponseLink.close();
    }
    
}