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

import org.elasticsearch.test.ESTestCase;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CancellableRateLimitedFluxIteratorTests extends ESTestCase {

    public void testConsumeAllElements() {
        List<Integer> numbers = List.of(1, 2, 3, 4, 5, 6, 7, 8);
        Flux<Integer> flux = Flux.fromIterable(numbers);

        Set<Integer> cleanedElements = new HashSet<>();
        CancellableRateLimitedFluxIterator<Integer> iterator =
            new CancellableRateLimitedFluxIterator<>(flux, 2, cleanedElements::add);

        int consumedElements = 0;
        while (iterator.hasNext()) {
            Integer number = numbers.get(consumedElements++);
            Integer next = iterator.next();
            assertThat(next, equalTo(number));
        }

        assertThat(consumedElements, equalTo(numbers.size()));
        assertThat(cleanedElements, is(empty()));
        assertThat(iterator.getQueue(), is(empty()));
    }

    public void testItRequestsUpstreamInBatches() {
        final int elementsPerBatch = randomIntBetween(4, 10);
        final int providedElements = randomIntBetween(0, elementsPerBatch - 1);
        Publisher<Integer> publisher = s -> runOnNewThread(() -> {
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    assertThat(n, equalTo((long) elementsPerBatch));
                    // Provide less elements than requested and complete
                    for (int i = 0; i < providedElements; i++) {
                        s.onNext(i);
                    }
                    s.onComplete();
                }

                @Override
                public void cancel() {

                }
            });
        });

        final Set<Integer> cleanedElements = new HashSet<>();
        final CancellableRateLimitedFluxIterator<Integer> iterator =
            new CancellableRateLimitedFluxIterator<>(publisher, elementsPerBatch, cleanedElements::add);

        final List<Integer> consumedElements = new ArrayList<>();
        while (iterator.hasNext()) {
            consumedElements.add(iterator.next());
        }

        assertThat(consumedElements.size(), equalTo(providedElements));
        // Elements are provided in order
        for (int i = 0; i < providedElements; i++) {
            assertThat(consumedElements.get(i), equalTo(i));
        }
        assertThat(cleanedElements, is(empty()));
        assertThat(iterator.getQueue(), is(empty()));
    }

    public void testErrorPath() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Publisher<Integer> publisher = s -> runOnNewThread(() -> {
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    assertThat(n, equalTo(2L));
                    s.onNext(1);
                    s.onNext(2);

                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        assert false;
                    }

                    runOnNewThread(() -> s.onError(new IOException("FAILED")));
                }

                @Override
                public void cancel() {

                }
            });
        });

        Set<Integer> cleaning = new HashSet<>();
        CancellableRateLimitedFluxIterator<Integer> iterator =
            new CancellableRateLimitedFluxIterator<>(publisher, 2, cleaning::add);

        assertThat(iterator.hasNext(), equalTo(true));
        assertThat(iterator.next(), equalTo(1));

        latch.countDown();
        //noinspection ResultOfMethodCallIgnored
        assertBusy(() -> expectThrows(RuntimeException.class, iterator::hasNext));
        assertThat(cleaning, equalTo(Set.of(2)));
        assertThat(iterator.getQueue(), is(empty()));
    }

    public void testCancellation() throws Exception {
        int requestedElements = 4;
        final AtomicBoolean cancelled = new AtomicBoolean();
        Publisher<Integer> publisher = s -> runOnNewThread(() -> {
            s.onSubscribe(new Subscription() {
                final CountDownLatch cancellationLatch = new CountDownLatch(1);
                final AtomicBoolean b = new AtomicBoolean(false);

                @Override
                public void request(long n) {
                    if (b.compareAndSet(false, true)) {
                        assertThat(n, equalTo((long) requestedElements));
                        s.onNext(1);
                        s.onNext(2);
                        try {
                            cancellationLatch.await();
                        } catch (InterruptedException e) {
                            assert false;
                        }


                        runOnNewThread(() -> {
                            // It's possible that extra elements are emitted after cancellation
                            s.onNext(3);
                            s.onNext(4);
                        });
                    }
                }

                @Override
                public void cancel() {
                    cancelled.set(true);
                    cancellationLatch.countDown();
                }
            });
        });
        Set<Integer> cleaning = new HashSet<>();
        CancellableRateLimitedFluxIterator<Integer> iterator =
            new CancellableRateLimitedFluxIterator<>(publisher, requestedElements, cleaning::add);

        assertThat(iterator.hasNext(), equalTo(true));
        assertThat(iterator.next(), equalTo(1));
        assertThat(iterator.next(), equalTo(2));
        iterator.cancel();
        assertThat(iterator.hasNext(), equalTo(false));

        assertBusy(() -> assertThat(cleaning, equalTo(Set.of(3, 4))));
        assertThat(iterator.getQueue(), is(empty()));
    }

    public void testConcurrentCancellationAndErrorOrCompletion() {
        fail("TODO");
    }

    public void runOnNewThread(Runnable runnable) {
        new Thread(runnable).start();
    }
}
