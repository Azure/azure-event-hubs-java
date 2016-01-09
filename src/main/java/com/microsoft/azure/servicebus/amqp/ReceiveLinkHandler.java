package com.microsoft.azure.servicebus.amqp;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnknownDescribedType;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.engine.Event.Type;
import org.apache.qpid.proton.message.Message;

import com.microsoft.azure.servicebus.ClientConstants;
import com.microsoft.azure.servicebus.MessageReceiver;

/** 
 * ServiceBus <-> ProtonReactor interaction 
 * handles all recvLink - reactor events
 */
public final class ReceiveLinkHandler extends BaseHandler
{
	private static final Logger TRACE_LOGGER = Logger.getLogger(ClientConstants.ServiceBusClientTrace);
	
	private final String name;
	private final MessageReceiver msgReceiver;
	private final Object firstFlow;
	private boolean isFirstFlow;
	
	public ReceiveLinkHandler(final String name, final MessageReceiver receiver)
	{
		this.name = name;
		this.msgReceiver = receiver;
		this.firstFlow = new Object();
		this.isFirstFlow = true;
	}
	
	@Override
    public void onUnhandled(Event event)
	{
		if(TRACE_LOGGER.isLoggable(Level.FINE))
        {
            TRACE_LOGGER.log(Level.FINE, "recvLink.onUnhandled: name[" + event.getLink().getName() + "] : event["+event+"]");
        }
    }	
	
	@Override
    public void onLinkLocalOpen(Event evt)
	{
        Link link = evt.getLink();
        if (link instanceof Receiver)
        {
            Receiver receiver = (Receiver) link;
            
            if(TRACE_LOGGER.isLoggable(Level.FINE))
            {
            	TRACE_LOGGER.log(Level.FINE,
            			String.format("LinkHandler(name: %s) initial credit: %s", this.name, receiver.getCredit()));
            }
            
            if (receiver.getCredit() < this.msgReceiver.getPrefetchCount())
            {
                receiver.flow(this.msgReceiver.getPrefetchCount());
            }
        }
    }
	
	@Override
    public void onLinkRemoteClose(Event event)
	{
		Link link = event.getLink();
        if (link instanceof Receiver)
        {
        	ErrorCondition condition = link.getRemoteCondition();
        	if (condition != null)
    		{
    			if(TRACE_LOGGER.isLoggable(Level.WARNING))
    	        {
    				TRACE_LOGGER.log(Level.WARNING, "recvLink.onLinkRemoteClose: name["+link.getName()+"] : ErrorCondition[" + condition.getCondition() + ", " + condition.getDescription() + "]");
    	        }
            } 
    		
    		this.msgReceiver.onError(condition);
        }
	}
	
	
	@Override
	public void onLinkRemoteDetach(Event event)
	{
		Link link = event.getLink();
        if (link instanceof Receiver)
        {
        	ErrorCondition condition = link.getRemoteCondition();
        	if (condition != null)
        	{
        		if (TRACE_LOGGER.isLoggable(Level.WARNING))
        		TRACE_LOGGER.log(Level.WARNING, "recvLink.onLinkRemoteDetach: name["+link.getName()+"] : ErrorCondition[" + condition.getCondition() + ", " + condition.getDescription() + "]");
            }
        	
        	this.msgReceiver.onError(condition);
            link.close();
        }
	}
	
	@Override
    public void onDelivery(Event event)
	{
		if (this.isFirstFlow)
        {
			synchronized (this.firstFlow)
			{
				if (this.isFirstFlow)
				{
					this.msgReceiver.onOpenComplete(null);
					this.isFirstFlow = false;
				}
			}
		}
        
        Delivery delivery = event.getDelivery();
        Receiver receiveLink = (Receiver) delivery.getLink();
        LinkedList<Message> messages = new LinkedList<Message>();
        
        while (delivery != null && delivery.isReadable() && !delivery.isPartial())
        {
        	int size = delivery.pending();
            byte[] buffer = new byte[size];
            int read = receiveLink.recv(buffer, 0, buffer.length);
            
            Message msg = Proton.message();
            msg.decode(buffer, 0, read);
            
            messages.add(msg);
            
            if (receiveLink.advance())
            {
            	delivery = receiveLink.current();
            }
            else
            {    
            	break;
            }
        }        
        
        if (messages != null && messages.size() > 0)
        {
        	this.msgReceiver.onDelivery(messages);
        }
        
        if(TRACE_LOGGER.isLoggable(Level.FINE))
        {
        	TRACE_LOGGER.log(Level.FINE,
        			String.format("recvLink.onDelivery (name: %s) credit: %s", this.name, receiveLink.getCredit()));
        }
    }
}
