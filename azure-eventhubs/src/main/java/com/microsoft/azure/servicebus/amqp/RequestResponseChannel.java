/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus.amqp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.EndpointState;

import com.microsoft.azure.servicebus.StringUtil;

public class RequestResponseChannel implements IIOObject {
    
    private final Sender sendLink;
    private final Receiver receiveLink;
    private final String replyTo;
    private final HashMap<Object, IOperationResult<Map<String, Object>, Exception>> inflightRequests;
    private final AtomicLong requestId;
    private final AtomicInteger openRefCount;
    private final AtomicInteger closeRefCount;
    
    private IOperationResult<Void, Exception> onOpen;
    private IOperationResult<Void, Exception> onClose;
    
    public RequestResponseChannel(
            final String linkName,
            final String path,
            final Session session) {
        
        this.replyTo = path.replace("$", "") + "-client-reply-to";
        this.openRefCount = new AtomicInteger(2);
        this.closeRefCount = new AtomicInteger(2);
        this.inflightRequests = new HashMap<>();
        this.requestId = new AtomicLong(0);
        
        this.sendLink = session.sender(linkName + ":sender");
        final Target target = new Target();
        target.setAddress(path);
        this.sendLink.setTarget(target);
        sendLink.setSource(new Source());
        this.sendLink.setSenderSettleMode(SenderSettleMode.SETTLED);
        BaseHandler.setHandler(this.sendLink, new SendLinkHandler(new RequestHandler()));
        
        this.receiveLink = session.receiver(linkName + ":receiver");
        final Source source = new Source();
        source.setAddress(path);
        this.receiveLink.setSource(source);
        final Target receiverTarget = new Target();
        receiverTarget.setAddress(this.replyTo);
        this.receiveLink.setTarget(receiverTarget);
        this.receiveLink.setSenderSettleMode(SenderSettleMode.SETTLED);
        this.receiveLink.setReceiverSettleMode(ReceiverSettleMode.SECOND);
        BaseHandler.setHandler(this.receiveLink, new ReceiveLinkHandler(new ResponseHandler()));
    }

    // open should be called only once - we use FaultTolerantObject for that
    public void open(final IOperationResult<Void, Exception> onOpen, final IOperationResult<Void, Exception> onClose) {
        
        this.onOpen = onOpen;
        this.onClose = onClose;
        this.sendLink.open();
        this.receiveLink.open();        
    }
    
    // close should be called exactly once - we use FaultTolerantObject for that
    public void close() {

        this.sendLink.close();
        this.receiveLink.close();
    }
    
    public Sender getSendLink() {
        return this.sendLink;
    }
    
    public Receiver getReceiveLink() {
        return this.receiveLink;
    }
    
    public void request(
            final ReactorDispatcher dispatcher,
            final Message message,
            final IOperationResult<Map<String, Object>, Exception> onResponse) {

        if (message == null)
            throw new IllegalArgumentException("message cannot be null");

        if (message.getMessageId() != null)
            throw new IllegalArgumentException("message.getMessageId() should be null");
        
        if (message.getReplyTo() != null)
            throw new IllegalArgumentException("message.getReplyTo() should be null");
        
        message.setMessageId("request" + UnsignedLong.valueOf(this.requestId.incrementAndGet()).toString());
        message.setReplyTo(this.replyTo);
        
        this.inflightRequests.put(message.getMessageId(), onResponse);
                
        try {
            dispatcher.invoke(new DispatchHandler() {
                @Override
                public void onEvent() {
                    
                    final Delivery delivery = sendLink.delivery(UUID.randomUUID().toString().replace("-", StringUtil.EMPTY).getBytes());
                    final int payloadSize = AmqpUtil.getDataSerializedSize(message) + 512; // need buffer for headers
                    
                    delivery.setContext(onResponse);
                    
                    final byte[] bytes = new byte[payloadSize];
                    final int encodedSize = message.encode(bytes, 0, payloadSize);
                    
                    receiveLink.flow(1);
                    sendLink.send(bytes, 0, encodedSize);
                    sendLink.advance();
                }
            });
        } catch (IOException ioException) {
            onResponse.onError(ioException);
        }
    }
    
    private void onLinkOpenComplete() {
        
        if (openRefCount.decrementAndGet() <= 0 && onOpen != null) {
            final Collection<Link> links = new LinkedList<>();
            links.add(sendLink);
            links.add(receiveLink);
            onOpen.onComplete(null);
        }
    }
    
    private void onLinkCloseComplete() {
        
        if (closeRefCount.decrementAndGet() <= 0)
                onClose.onComplete(null);
    }

    @Override
    public IOObjectState getState() {
        
        if (sendLink.getLocalState() == EndpointState.UNINITIALIZED || receiveLink.getLocalState() == EndpointState.UNINITIALIZED
                || sendLink.getRemoteState() == EndpointState.UNINITIALIZED || receiveLink.getRemoteState() == EndpointState.UNINITIALIZED)
            return IOObjectState.OPENING;
        
        if (sendLink.getRemoteState() == EndpointState.ACTIVE && receiveLink.getRemoteState() == EndpointState.ACTIVE
                && sendLink.getLocalState() == EndpointState.ACTIVE && receiveLink.getRemoteState() == EndpointState.ACTIVE)
            return IOObjectState.OPENED;
        
        return IOObjectState.CLOSED;
    }
    
    private class RequestHandler implements IAmqpSender {

        @Override
        public void onFlow(int creditIssued) {
        }

        @Override
        public void onSendComplete(Delivery delivery) {
        }

        @Override
        public void onOpenComplete(Exception completionException) {
            
            if (completionException != null && onOpen != null) {
                onOpen.onError(completionException);
                onOpen = null; // to avoid invoking twice
            }
            else
                onLinkOpenComplete();
        }

        @Override
        public void onError(Exception exception) {
            
            if (onClose != null) {
                onClose.onError(exception);
                onClose = null; // to avoid invoking twice
            }
        }

        @Override
        public void onClose(ErrorCondition condition) {
            
            if (condition == null)
                onLinkCloseComplete();
            else
                onError(new AmqpException(condition));
        }        
        
    }
    
    private class ResponseHandler implements IAmqpReceiver {

        @Override
        public void onReceiveComplete(Delivery delivery) {
            
            final Message response = Proton.message();
            final int msgSize = delivery.pending();
            final byte[] buffer = new byte[msgSize];

            final int read = receiveLink.recv(buffer, 0, msgSize);

            response.decode(buffer, 0, read);
            delivery.settle();
            
            final int statusCode = (int) response.getApplicationProperties().getValue().get(AmqpConstants.MANAGEMENT_STATUS_CODE_KEY);
            final String statusDescription = (String) response.getApplicationProperties().getValue().get(AmqpConstants.MANAGEMENT_STATUS_DESCRIPTION_KEY);
            
            final IOperationResult<Map<String, Object>, Exception> responseCallback = inflightRequests.remove(response.getCorrelationId());
            if (responseCallback != null) {
                
                if (statusCode == AmqpManagementResponseCode.ACCEPTED.getValue() || statusCode == AmqpManagementResponseCode.OK.getValue()) {
                    
                    if (response.getBody() == null)
                        responseCallback.onComplete(null);
                    else
                        responseCallback.onComplete((Map<String, Object>) ((AmqpValue) response.getBody()).getValue());
                }
                else {

                    final Symbol condition = (Symbol) response.getApplicationProperties().getValue().get(AmqpConstants.MANAGEMENT_RESPONSE_ERROR_CONDITION);
                    final ErrorCondition error = new ErrorCondition(condition, statusDescription);
                    responseCallback.onError(new AmqpException(error));
                }
            }
        }

        @Override
        public void onOpenComplete(Exception completionException) {
            
            if (completionException != null && onOpen != null)
                onOpen.onError(completionException);
            else
                onLinkOpenComplete();
        }

        @Override
        public void onError(Exception exception) {
            
            for (IOperationResult<Map<String, Object>, Exception> responseCallback: inflightRequests.values())
                responseCallback.onError(exception);
            
            inflightRequests.clear();
            
            if (onClose != null) {
                onClose.onError(exception);
                onClose = null; //to avoid closing twice
            }
        }

        @Override
        public void onClose(ErrorCondition condition) {
            
            if (condition == null)
                onLinkCloseComplete();
            else
                onError(new AmqpException(condition));
        }        
    }    
}
