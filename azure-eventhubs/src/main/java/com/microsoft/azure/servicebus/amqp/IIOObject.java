/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.microsoft.azure.servicebus.amqp;

public interface IIOObject {

    public static enum IOObjectState {
        OPENING,
        OPENED,
        CLOSED
    }

    // should be run on reactor thread
    public IOObjectState getState();
}
