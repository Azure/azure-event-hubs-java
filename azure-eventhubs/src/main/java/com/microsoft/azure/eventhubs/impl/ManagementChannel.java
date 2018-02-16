/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.message.Message;

final class ManagementChannel {

    final FaultTolerantObject<RequestResponseChannel> innerChannel;
    final SessionProvider sessionProvider;
    final AmqpConnection connectionEventDispatcher;

    public ManagementChannel(final SessionProvider sessionProvider, final AmqpConnection connection,
                             final String linkName) {
        this.sessionProvider = sessionProvider;
        this.connectionEventDispatcher = connection;

        RequestResponseCloser closer = new RequestResponseCloser();
        this.innerChannel = new FaultTolerantObject<>(
        		new RequestResponseOpener(sessionProvider, "mgmt-session", "mgmt", ClientConstants.MANAGEMENT_ADDRESS, connection),
                closer);
        closer.setInnerChannel(this.innerChannel);
    }
	
	public CompletableFuture<Map<String, Object>> request(final ReactorDispatcher dispatcher, final Map<String, Object> request)
	{
		final Message requestMessage = Proton.message();
        final ApplicationProperties applicationProperties = new ApplicationProperties(request);
        requestMessage.setApplicationProperties(applicationProperties);
        // no body required
        
        CompletableFuture<Map<String, Object>> resultFuture = new CompletableFuture<Map<String, Object>>();
        
        this.innerChannel.runOnOpenedObject(dispatcher,
                new OperationResult<RequestResponseChannel, Exception>() {
                    @Override
                    public void onComplete(final RequestResponseChannel result) {
                        result.request(requestMessage,
                                new OperationResult<Message, Exception>() {
                                    @Override
                                    public void onComplete(final Message response) {
                                        final int statusCode = (int)response.getApplicationProperties().getValue().get(ClientConstants.PUT_TOKEN_STATUS_CODE);
                                        final String statusDescription = (String)response.getApplicationProperties().getValue().get(ClientConstants.PUT_TOKEN_STATUS_DESCRIPTION);

                                        if (statusCode == AmqpResponseCode.ACCEPTED.getValue() || statusCode == AmqpResponseCode.OK.getValue()) {
                                            if (response.getBody() != null) {
                                                resultFuture.complete((Map<String, Object>)((AmqpValue)response.getBody()).getValue());
                                            }
                                        } 
                                        else {
                                            this.onError(ExceptionUtil.amqpResponseCodeToException(statusCode, statusDescription));
                                        }
                                    }

                                    @Override
                                    public void onError(final Exception error) {
                                    	resultFuture.completeExceptionally(error);
                                    }
                                });
                    }

                    @Override
                    public void onError(Exception error) {
                        resultFuture.completeExceptionally(error);
                    }
                });
        
		return resultFuture;
	}
	
    public void close(final ReactorDispatcher reactorDispatcher, final OperationResult<Void, Exception> closeCallback) {
        this.innerChannel.close(reactorDispatcher, closeCallback);
    }
}
