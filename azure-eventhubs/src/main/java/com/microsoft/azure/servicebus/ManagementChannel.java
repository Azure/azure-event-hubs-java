/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import com.microsoft.azure.servicebus.ClientConstants;
import com.microsoft.azure.servicebus.FaultTolerantObject;
import com.microsoft.azure.servicebus.amqp.AmqpException;
import com.microsoft.azure.servicebus.amqp.AmqpResponseCode;
import com.microsoft.azure.servicebus.amqp.IAmqpConnection;
import com.microsoft.azure.servicebus.amqp.IOperation;
import com.microsoft.azure.servicebus.amqp.IOperationResult;
import com.microsoft.azure.servicebus.amqp.ReactorDispatcher;
import com.microsoft.azure.servicebus.amqp.RequestResponseChannel;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.message.Message;

public class ManagementChannel {
    final FaultTolerantObject<RequestResponseChannel> innerChannel;
    final ISessionProvider sessionProvider;
    final IAmqpConnection connectionEventDispatcher;

    public ManagementChannel(final ISessionProvider sessionProvider, final IAmqpConnection connection,
            final String linkName) {
        this.sessionProvider = sessionProvider;
        this.connectionEventDispatcher = connection;

        this.innerChannel = new FaultTolerantObject<>(
                new OpenRequestResponseChannel(),
                new CloseRequestResponseChannel());
    }
	
	public CompletableFuture<Map<String, Object>> doManagementRequestResponse(final ReactorDispatcher dispatcher, final Map<String, String> request)
	{
		final Message requestMessage = Proton.message();
        final ApplicationProperties applicationProperties = new ApplicationProperties(request);
        requestMessage.setApplicationProperties(applicationProperties);
        // no body required
        
        CompletableFuture<Map<String, Object>> resultFuture = new CompletableFuture<Map<String, Object>>();
        
        this.innerChannel.runOnOpenedObject(dispatcher,
                new IOperationResult<RequestResponseChannel, Exception>() {
                    @Override
                    public void onComplete(final RequestResponseChannel result) {
                        result.request(dispatcher, requestMessage,
                                new IOperationResult<Message, Exception>() {
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

    public void close(final ReactorDispatcher reactorDispatcher, final IOperationResult<Void, Exception> closeCallback) {
        this.innerChannel.close(reactorDispatcher, closeCallback);
    }

    private class OpenRequestResponseChannel implements IOperation<RequestResponseChannel> {
        @Override
        public void run(IOperationResult<RequestResponseChannel, Exception> operationCallback) {
            final Session session = ManagementChannel.this.sessionProvider.getSession(
                    "path",
                    null,
                    new BiConsumer<ErrorCondition, Exception>() {
                        @Override
                        public void accept(ErrorCondition error, Exception exception) {
                            if (error != null)
                                operationCallback.onError(new AmqpException(error));
                            else if (exception != null)
                                operationCallback.onError(exception);
                        }
                    });

            if (session == null)
                return;

            final RequestResponseChannel requestResponseChannel = new RequestResponseChannel(
                    "mgmt",
                    ClientConstants.MANAGEMENT_ADDRESS,
                    session);

            requestResponseChannel.open(
                    new IOperationResult<Void, Exception>() {
                        @Override
                        public void onComplete(Void result) {
                            connectionEventDispatcher.registerForConnectionError(requestResponseChannel.getSendLink());
                            connectionEventDispatcher.registerForConnectionError(requestResponseChannel.getReceiveLink());

                            operationCallback.onComplete(requestResponseChannel);
                        }

                        @Override
                        public void onError(Exception error) {
                            operationCallback.onError(error);
                        }
                    },
                    new IOperationResult<Void, Exception>() {
                        @Override
                        public void onComplete(Void result) {
                            connectionEventDispatcher.deregisterForConnectionError(requestResponseChannel.getSendLink());
                            connectionEventDispatcher.deregisterForConnectionError(requestResponseChannel.getReceiveLink());
                        }

                        @Override
                        public void onError(Exception error) {
                            connectionEventDispatcher.deregisterForConnectionError(requestResponseChannel.getSendLink());
                            connectionEventDispatcher.deregisterForConnectionError(requestResponseChannel.getReceiveLink());
                        }
                    });
        }
    }

    private class CloseRequestResponseChannel implements IOperation<Void> {
        @Override
        public void run(IOperationResult<Void, Exception> closeOperationCallback) {
            final RequestResponseChannel channelToBeClosed = innerChannel.unsafeGetIfOpened();
            if (channelToBeClosed == null) {
                closeOperationCallback.onComplete(null);
            }
            else {
                channelToBeClosed.close(new IOperationResult<Void, Exception>() {
                    @Override
                    public void onComplete(Void result) {
                        closeOperationCallback.onComplete(result);
                    }

                    @Override
                    public void onError(Exception error) {
                        closeOperationCallback.onError(error);
                    }
                });
            }
        }
    }
}
