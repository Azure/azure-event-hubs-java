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
public interface IOperation<T> {
    
    public void run(IOperationResult<T, Exception> operationCallback);
}
