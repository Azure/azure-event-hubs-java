package com.microsoft.azure.eventhubs;

import com.microsoft.azure.eventhubs.amqp.ReactorDispatcher;

public interface ISchedulerProvider {
    ReactorDispatcher getReactorScheduler();
}
