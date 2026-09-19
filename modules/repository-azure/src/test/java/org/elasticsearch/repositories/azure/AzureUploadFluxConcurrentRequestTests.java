/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import com.azure.storage.common.Utility;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

/**
 * The upload flux of {@link AzureBlobStore} is consumed by reactor-netty's {@code MonoSendMany}, which requests 128 buffers up front and
 * 64 more from the netty event loop each time its outstanding demand drops to 64. {@code subscribeOn} runs every such request as a task on
 * its scheduler worker, and when the pool that runs them is busy two refills can be queued and then start on two pool threads at the same
 * instant, which Reactive Streams rule 2.7 forbids upstream.
 * <p>
 * Two things keep that from corrupting an upload, each tested on its own here with {@code request} pairs released together from two
 * threads. {@link AzureClientProvider} installs a {@code boundedElastic} scheduler whose workers serialize their tasks, so the requests
 * never overlap even for a {@code concatMap} chain, whose parked one-shot scalar would otherwise emit a buffer twice and skip the next.
 * And the SDK's {@link Utility#convertStreamToByteBuffer}, which {@link AzureBlobStore} reads its streams with, is built on
 * {@link Flux#generate}, which delivers the right sequence even when the requests do overlap on a scheduler that does not serialize them.
 */
public class AzureUploadFluxConcurrentRequestTests extends ESTestCase {

    private static final int MAX_SIZE = 128;
    private static final int CHUNK_SIZE = 1024;
    private static final int CHUNKS = 20_000;
    private static final int ITERATIONS = 3;

    private ThreadPool threadPool;
    private ExecutorService requesters;

    @Before
    public void startPools() {
        threadPool = new TestThreadPool(
            getTestClass().getName(),
            AzureRepositoryPlugin.executorBuilder(Settings.EMPTY),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        requesters = Executors.newFixedThreadPool(2, r -> new Thread(r, "fake-event-loop"));
    }

    @After
    public void stopPools() {
        Schedulers.resetFactory();
        requesters.shutdownNow();
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    /**
     * The scheduler installed for the SDK by {@link AzureClientProvider} serializes the requests of a subscription: even a chain that
     * cannot cope with concurrent requests delivers the right sequence, and none of the paired requests overlap upstream of subscribeOn.
     */
    public void testInstalledBoundedElasticSchedulerSerializesRequests() throws Exception {
        final AzureClientProvider clientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY);
        clientProvider.start();
        try {
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                final Run run = new Run(AzureUploadFluxConcurrentRequestTests::concatMapChunks, Schedulers.boundedElastic());
                run.execute();
                assertNull(run.describeFailure());
                assertThat("requests overlapped upstream of subscribeOn", run.overlappingRequests.get(), equalTo(0));
            }
        } finally {
            clientProvider.close();
        }
    }

    /**
     * The SDK's stream reader delivers the right sequence even when {@code boundedElastic} is backed by a scheduler that does not
     * serialize the requests of a subscription, so that the paired requests reach it concurrently.
     */
    public void testSdkStreamReaderToleratesConcurrentRequests() throws Exception {
        final Scheduler nonSerializing = Schedulers.fromExecutor(threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME));
        Schedulers.setFactory(new Schedulers.Factory() {
            @Override
            public Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
                return nonSerializing;
            }
        });
        try {
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                // the SDK method subscribes on boundedElastic itself
                final Run run = new Run(
                    stream -> Utility.convertStreamToByteBuffer(stream, (long) CHUNKS * CHUNK_SIZE, CHUNK_SIZE, false),
                    null
                );
                run.execute();
                assertNull(run.describeFailure());
            }
        } finally {
            Schedulers.resetFactory();
            nonSerializing.dispose();
        }
    }

    /**
     * The shape {@link AzureBlobStore} used before it moved to the SDK's reader: a {@code concatMap} over {@link Mono#fromCallable} reads.
     * It parks each buffer it has read in a one-shot scalar subscription guarded by a plain flag, which two concurrent requests can both
     * fire.
     */
    private static Flux<ByteBuffer> concatMapChunks(InputStream stream) {
        return Flux.range(0, CHUNKS).map(i -> (long) i * CHUNK_SIZE).concatMap(pos -> Mono.fromCallable(() -> {
            final byte[] buffer = new byte[CHUNK_SIZE];
            int offset = 0;
            while (offset < CHUNK_SIZE) {
                final int read = stream.read(buffer, offset, CHUNK_SIZE - offset);
                if (read == -1) {
                    throw new IllegalStateException("stream ended at " + (pos + offset));
                }
                offset += read;
            }
            return ByteBuffer.wrap(buffer);
        }));
    }

    /**
     * One upload: the consumer takes {@link #MAX_SIZE} buffers up front, then asks for the rest one pair at a time, each pair as two
     * {@code request(1)} calls released together from two threads once the producer has caught up with the previous pair.
     */
    private final class Run {
        private final Function<InputStream, Flux<ByteBuffer>> chunks;
        private final Scheduler subscribeOn;
        private final AtomicInteger requestsInFlight = new AtomicInteger();
        private final AtomicInteger overlappingRequests = new AtomicInteger();
        private final ConcurrentLinkedQueue<Integer> received = new ConcurrentLinkedQueue<>();
        private final AtomicInteger receivedCount = new AtomicInteger();
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);
        private int stuckAt = -1;
        private boolean completed;

        /**
         * @param chunks      builds the flux under test from the upload stream
         * @param subscribeOn scheduler to subscribe on after the overlap detector, or {@code null} when {@code chunks} subscribes on one itself
         */
        Run(Function<InputStream, Flux<ByteBuffer>> chunks, Scheduler subscribeOn) {
            this.chunks = chunks;
            this.subscribeOn = subscribeOn;
        }

        void execute() throws Exception {
            final long length = (long) CHUNKS * CHUNK_SIZE;
            Flux<ByteBuffer> flux = Flux.using(() -> new ChunkStream(length), stream -> {
                final Flux<ByteBuffer> reads = chunks.apply(stream);
                // sits right above the flux under test, and counts request calls that overlap in time
                return Flux.<ByteBuffer>from(actual -> reads.subscribe(new CoreSubscriber<ByteBuffer>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        actual.onSubscribe(new Subscription() {
                            @Override
                            public void request(long n) {
                                if (requestsInFlight.incrementAndGet() > 1) {
                                    overlappingRequests.incrementAndGet();
                                }
                                try {
                                    s.request(n);
                                } finally {
                                    requestsInFlight.decrementAndGet();
                                }
                            }

                            @Override
                            public void cancel() {
                                s.cancel();
                            }
                        });
                    }

                    @Override
                    public void onNext(ByteBuffer buffer) {
                        actual.onNext(buffer);
                    }

                    @Override
                    public void onError(Throwable t) {
                        actual.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        actual.onComplete();
                    }
                }));
            }, IOUtils::closeWhileHandlingException);
            if (subscribeOn != null) {
                flux = flux.subscribeOn(subscribeOn);
            }

            final PairedRequestSubscriber subscriber = new PairedRequestSubscriber();
            flux.subscribe(subscriber);
            assertTrue("initial batch was not delivered", subscriber.awaitDelivered(MAX_SIZE));
            int delivered = MAX_SIZE;
            while (delivered < CHUNKS && error.get() == null) {
                final int expected = Math.min(CHUNKS, delivered + 2);
                subscriber.requestPair();
                if (subscriber.awaitDelivered(expected) == false) {
                    // a dropped buffer leaves the consumer one short: ask again so the run can finish and report the sequence
                    subscriber.requestPair();
                    if (subscriber.awaitDelivered(expected) == false) {
                        stuckAt = receivedCount.get();
                        break;
                    }
                }
                delivered = receivedCount.get();
            }
            completed = done.await(10, TimeUnit.SECONDS);
        }

        String describeFailure() {
            if (error.get() != null) {
                return "failed: " + error.get();
            }
            final List<Integer> sequence = new ArrayList<>(received);
            final StringBuilder description = new StringBuilder();
            if (stuckAt >= 0) {
                description.append("\n  consumer stuck at ")
                    .append(stuckAt)
                    .append(" delivered chunks, nothing more arrived for its requests");
            }
            if (completed == false) {
                description.append("\n  onComplete never arrived");
            }
            if (sequence.size() != CHUNKS) {
                description.append("\n  received ").append(sequence.size()).append(" chunks instead of ").append(CHUNKS);
            }
            final int[] positions = new int[CHUNKS];
            Arrays.fill(positions, -1);
            for (int i = 0; i < sequence.size(); i++) {
                final int chunk = sequence.get(i);
                if (chunk < 0 || chunk >= CHUNKS) {
                    description.append("\n  position ").append(i).append(" holds unknown chunk ").append(chunk);
                } else if (positions[chunk] >= 0) {
                    description.append("\n  chunk ")
                        .append(chunk)
                        .append(" DUPLICATED: delivered at positions ")
                        .append(positions[chunk])
                        .append(" and ")
                        .append(i);
                } else {
                    positions[chunk] = i;
                }
            }
            for (int chunk = 0; chunk < CHUNKS; chunk++) {
                if (positions[chunk] < 0) {
                    description.append("\n  chunk ").append(chunk).append(" DROPPED: never delivered");
                }
            }
            if (description.isEmpty()) {
                return null;
            }
            return "wrong sequence (" + overlappingRequests.get() + " overlapping upstream request() calls):" + description;
        }

        /**
         * Requests {@link #MAX_SIZE} on subscribe. {@link #requestPair()} then releases two {@code request(1)} calls from two threads at
         * the same instant, like two refills whose tasks were queued behind other work and start together.
         */
        private final class PairedRequestSubscriber extends BaseSubscriber<ByteBuffer> {

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(MAX_SIZE);
            }

            @Override
            protected void hookOnNext(ByteBuffer buffer) {
                received.add(buffer.getInt(0));
                receivedCount.incrementAndGet();
            }

            @Override
            protected void hookOnComplete() {
                done.countDown();
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                error.set(throwable);
                done.countDown();
            }

            void requestPair() throws Exception {
                final CyclicBarrier barrier = new CyclicBarrier(2);
                final CountDownLatch issued = new CountDownLatch(2);
                for (int i = 0; i < 2; i++) {
                    requesters.execute(() -> {
                        try {
                            barrier.await(10, TimeUnit.SECONDS);
                            request(1);
                        } catch (Exception e) {
                            error.compareAndSet(null, e);
                        } finally {
                            issued.countDown();
                        }
                    });
                }
                assertTrue(issued.await(10, TimeUnit.SECONDS));
            }

            boolean awaitDelivered(int count) {
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (receivedCount.get() < count && error.get() == null) {
                    if (System.nanoTime() > deadline) {
                        return false;
                    }
                    Thread.onSpinWait();
                }
                return receivedCount.get() >= count;
            }
        }
    }

    /**
     * Sequential stream whose chunk {@code i} starts with the big-endian int {@code i}.
     */
    private static final class ChunkStream extends InputStream {
        private final long length;
        private long position;

        ChunkStream(long length) {
            this.length = length;
        }

        @Override
        public int read() throws IOException {
            final byte[] b = new byte[1];
            return read(b, 0, 1) == -1 ? -1 : b[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (position >= length) {
                return -1;
            }
            final int n = (int) Math.min(len, length - position);
            for (int i = 0; i < n; i++) {
                final long p = position + i;
                final int chunk = (int) (p / CHUNK_SIZE);
                final int inChunk = (int) (p % CHUNK_SIZE);
                b[off + i] = inChunk < Integer.BYTES ? (byte) (chunk >>> (8 * (3 - inChunk))) : (byte) (chunk + inChunk);
            }
            position += n;
            return n;
        }
    }
}
