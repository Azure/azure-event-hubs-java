package com.microsoft.azure.servicebus;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.*;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.message.Message;

/**
 * Abstracts all amqp related details
 * translates event-driven reactor model into async send Api
 */
public class MessageSender extends ClientEntity {
	
	public static final int MaxMessageLength = 4 * 1024;
	
	private final Object lock = new Object(); 
	private final Sender sendLink;
	private final ConcurrentHashMap<byte[], CompletableFuture<Void>> pendingSendWaiters;
	private final MessagingFactory underlyingFactory;
	private final String sendPath;
	
	private CompletableFuture<MessageSender> linkOpen; 
	
	private AtomicLong nextTag;
	private boolean firstSendFlowRecieved = false;
	
	public static CompletableFuture<MessageSender> Create(
			final MessagingFactory factory,
			final String sendLinkName,
			final String senderPath) throws EntityNotFoundException
	{
		MessageSender msgSender = new MessageSender(factory, sendLinkName, senderPath);
		SendLinkHandler handler = new SendLinkHandler(sendLinkName, msgSender);
		BaseHandler.setHandler(msgSender.sendLink, handler);
		return msgSender.linkOpen;
	}
	
	private MessageSender(final MessagingFactory factory, final String sendLinkName, final String senderPath) {
		this.sendPath = senderPath;
		this.underlyingFactory = factory;
		this.sendLink = MessageSender.createSendLink(factory.getConnection(), sendLinkName, senderPath);
		this.linkOpen = new CompletableFuture<MessageSender>();
		this.pendingSendWaiters = new ConcurrentHashMap<byte[], CompletableFuture<Void>>();
		this.nextTag = new AtomicLong(0);
	}
	
	public String getSendPath() {
		return this.sendPath;
	}
	
	// TODO: just enqueue on send and a timer which actually drains as many sends as getCredit() in that interval
	public CompletableFuture<Void> Send(Message msg) {
		
		byte[] bytes = new byte[5 * 1024];
		int encodedSize = msg.encode(bytes, 0, MaxMessageLength);
		
		byte[] tag = String.valueOf(nextTag.incrementAndGet()).getBytes();
        Delivery dlv = this.sendLink.delivery(tag);
        int sentMsgSize = this.sendLink.send(bytes, 0, encodedSize);
        
        assert sentMsgSize != encodedSize : "Contract of the ProtonJ library for Sender.Send API changed";
        
        CompletableFuture<Void> onSend = new CompletableFuture<Void>();
        this.pendingSendWaiters.put(tag, onSend);
        this.sendLink.advance();
        return onSend;
	}
	
	@Override
	public void close() {
		if (this.sendLink != null && this.sendLink.getLocalState() == EndpointState.ACTIVE) {
			this.sendLink.close();
		}
	}
	
	void onOpenComplete(ErrorCondition condition) {
		if (condition == null) {
			this.linkOpen.complete(this);
		}
		else {		
			this.linkOpen.completeExceptionally(ExceptionUtil.toException(condition));
		}
	}
	
	void onError(ErrorCondition error){
		synchronized (this.linkOpen) {
			if (!this.linkOpen.isDone()) {
				this.onOpenComplete(error);
				return;
			}
		}
		
		// TODO: what happens to Pending Sends
	}
	
	void onSendComplete(byte[] deliveryTag, DeliveryState outcome) {
		if (outcome == Accepted.getInstance()) {
			this.pendingSendWaiters.get(deliveryTag).complete(null);
		}
	}

	private static Sender createSendLink(final Connection connection, final String linkName, final String senderPath) {
		Session session = connection.session();
        session.open();
        
        Sender sender = session.sender(linkName);
        
        Target target = new Target();
        target.setAddress(senderPath);
        sender.setTarget(target);
        
        Source source = new Source();
        sender.setSource(source);
        
        sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        
        sender.open();
        return sender;
	}
}
