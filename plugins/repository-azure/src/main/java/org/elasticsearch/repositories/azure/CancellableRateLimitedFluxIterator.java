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

package org.elasticsearch.repositories.azure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class CancellableRateLimitedFluxIterator<T> implements Subscriber<T>, Iterator<T> {
    private static final Subscription CANCELLED_SUBSCRIPTION = new Subscription() {
        @Override
        public void request(long n) {
            // no op
        }

        @Override
        public void cancel() {
            // no op
        }
    };

    private final int elementsPerBatch;
    private final Queue<T> queue;
    // Used to provide visibility guarantees between producer/consumer
    private final Lock lock;
    // Used to signal consumers
    private final Condition condition;
    private final Consumer<T> cleaner;
    private final AtomicReference<Subscription> subscription = new AtomicReference<>();
    private final Logger logger = LogManager.getLogger(CancellableRateLimitedFluxIterator.class);
    private volatile Throwable error;
    private volatile boolean done;
    private int emittedElements;

    public CancellableRateLimitedFluxIterator(Publisher<T> byteBufFlux, int elementsPerBatch, Consumer<T> cleaner) {
        this.elementsPerBatch = elementsPerBatch;
        this.queue = new ArrayDeque<>(elementsPerBatch);
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.cleaner = cleaner;
        byteBufFlux.subscribe(this);
    }

    @Override
    public boolean hasNext() {
        for (; ; ) {
            // done is volatile and it's only set after completion, after error (meaning that this won't receive more data)
            // or after the iterator has been canceled (where we clear the queue before done is set to true)
            boolean isDone = done;
            boolean isQueueEmpty = queue.isEmpty();

            if (isDone) {
                Throwable e = error;
                if (e != null) {
                    throw new RuntimeException(e);
                } else if (isQueueEmpty) {
                    return false;
                }
            }

            if (isQueueEmpty == false) {
                return true;
            }

            // Provide visibility guarantees for the modified queue
            lock.lock();
            try {
                while (done == false && queue.isEmpty()) {
                    condition.await();
                }
            } catch (InterruptedException e) {
                cancelSubscription();
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public T next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        T nextElement = queue.poll();

        if (nextElement == null) {
            cancelSubscription();
            signalConsumer();

            throw new IllegalStateException("Queue is empty: Expected one element to be available from the Reactive Streams source.");
        }

        int p = emittedElements + 1;
        if (p == elementsPerBatch) {
            emittedElements = 0;
            subscription.get().request(p);
        }
        else {
            emittedElements = p;
        }

        return nextElement;
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (subscription.compareAndSet(null, s)) {
            s.request(elementsPerBatch);
        } else {
            s.cancel();
        }
    }

    @Override
    public void onNext(T refCounted) {
        // It's possible that we receive more elements after cancelling the subscription
        // since it might have outstanding requests before the cancellation. In that case
        // we just clean the resources.
        if (done) {
            cleanElement(refCounted);
            return;
        }

        boolean added = queue.offer(refCounted);
        // This should not happen unless we use a bounded queue, but it doesn't hurt to check
        assert added : "Queue is full: Reactive Streams source doesn't respect backpressure";
        signalConsumer();
    }

    public void cancel() {
        cancelSubscription();
        clearQueue();
        done = true;
        // cancel should be called from the consumer
        // thread, but to avoid potential deadlocks
        // we just try to release a possibly blocked
        // consumer
        signalConsumer();
    }

    @Override
    public void onError(Throwable t) {
        clearQueue();
        error = t;
        done = true;
        signalConsumer();
    }

    @Override
    public void onComplete() {
        done = true;
        signalConsumer();
    }

    // visible for testing
    Queue<T> getQueue() {
        return queue;
    }

    private void signalConsumer() {
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void clearQueue() {
        T element;
        while ((element = queue.poll()) != null) {
            cleanElement(element);
        }
    }

    private void cleanElement(T element) {
        try {
            cleaner.accept(element);
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("Unable to clean element"), e);
        }
    }

    private void cancelSubscription() {
        Subscription previousSubscription = subscription.getAndSet(CANCELLED_SUBSCRIPTION);
        previousSubscription.cancel();
    }
}
