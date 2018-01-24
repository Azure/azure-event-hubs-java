/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.eventhubs.amqp.DispatchHandler;
import com.microsoft.azure.eventhubs.amqp.ReactorDispatcher;

final class Timer {

    private Timer() {
    }

    public static CompletableFuture<?> schedule(
            final ReactorDispatcher dispatcher,
            final Runnable runnable,
            final Duration runAfter) {

        final ScheduledTask scheduledTask = new ScheduledTask(runnable);
        final CompletableFuture<?> taskHandle = scheduledTask.getScheduledFuture();
        try {
            dispatcher.invoke((int) runAfter.toMillis(), scheduledTask);
        } catch (IOException e) {
            taskHandle.completeExceptionally(e);
        }

        return taskHandle;
    }

    final static class ScheduledTask extends DispatchHandler {

        final CompletableFuture<?> scheduledFuture;
        final Runnable runnable;

        public ScheduledTask(final Runnable runnable) {
            this.runnable = runnable;
            this.scheduledFuture = new CompletableFuture<>();
        }

        @Override
        public void onEvent() {
            if (!scheduledFuture.isCancelled()) {
                try {
                    runnable.run();
                    scheduledFuture.complete(null);
                } catch (Exception exception) {
                    scheduledFuture.completeExceptionally(exception);
                }
            }
        }

        public CompletableFuture<?> getScheduledFuture() {
            return this.scheduledFuture;
        }
    }
}
