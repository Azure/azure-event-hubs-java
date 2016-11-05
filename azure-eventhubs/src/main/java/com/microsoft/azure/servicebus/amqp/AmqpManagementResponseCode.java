/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.servicebus.amqp;

/**
 *
 * @author sreeramg
 */
public enum AmqpManagementResponseCode
{
    ACCEPTED (0xca),
    OK (200);
    
    private final int value;
    private AmqpManagementResponseCode(final int value)
    {
        this.value = value;
    }
    
    public int getValue()
    {
        return this.value;
    }
}
