/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.azure.executors;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper around {@link ThreadPool} that provides the necessary scheduling methods for a {@link reactor.core.scheduler.Scheduler} to
 * function. This allows injecting a custom Executor to the reactor schedulers factory and avoid fine grained control over the
 * thread resources used.
 */
public class ReactorScheduledExecutorService extends AbstractExecutorService implements ScheduledExecutorService {
    private final ThreadPool threadPool;
    private final String executorName;
    private final ExecutorService delegate;

    public ReactorScheduledExecutorService(ThreadPool threadPool, String executorName) {
        this.threadPool = threadPool;
        this.executorName = executorName;
        this.delegate = threadPool.executor(executorName);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        Scheduler.ScheduledCancellable schedule = threadPool.schedule(() -> {
            try {
                decorateCallable(callable).call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, new TimeValue(delay, unit), executorName);

        return new ReactorFuture<>(schedule);
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        Runnable decoratedCommand = decorateRunnable(command);
        Scheduler.ScheduledCancellable schedule = threadPool.schedule(decoratedCommand, new TimeValue(delay, unit), executorName);
        return new ReactorFuture<>(schedule);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        Runnable decoratedCommand = decorateRunnable(command);

        Scheduler.Cancellable cancellable = threadPool.scheduleWithFixedRate(decoratedCommand,
            new TimeValue(initialDelay, unit),
            new TimeValue(period, unit),
            executorName);

        return new ReactorFuture<>(cancellable);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        Runnable decorateRunnable = decorateRunnable(command);

        Scheduler.Cancellable cancellable = threadPool.scheduleWithFixedDelay(decorateRunnable,
            new TimeValue(delay, unit),
            executorName);

        return new ReactorFuture<>(cancellable);
    }

    @Override
    public void shutdown() {
        // No-op
    }

    @Override
    public List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(decorateRunnable(command));
    }

    protected Runnable decorateRunnable(Runnable command) {
        return command;
    }

    protected <V> Callable<V> decorateCallable(Callable<V> callable) {
        return callable;
    }

    private static final class ReactorFuture<V> implements ScheduledFuture<V> {
        private final Scheduler.Cancellable cancellable;

        public ReactorFuture(Scheduler.Cancellable cancellable) {
            this.cancellable = cancellable;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(Delayed o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return cancellable.cancel();
        }

        @Override
        public boolean isCancelled() {
            return cancellable.isCancelled();
        }

        @Override
        public boolean isDone() {
            return cancellable.isCancelled();
        }

        @Override
        public V get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public V get(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    }
}
