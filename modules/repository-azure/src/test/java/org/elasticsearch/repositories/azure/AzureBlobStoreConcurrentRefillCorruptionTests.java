/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;

/**
 * Reproduces the BCC corruption of incident stateless_commit_3431 through {@link AzureBlobStore#writeBlobAtomic}: a blob stored by a single
 * {@code PUT Blob} in which one 64KB upload buffer appears twice and the following one is missing, with the length intact.
 * <p>
 * The upload {@code Flux} reads the stream in bursts of 64 buffers, each burst produced by one {@code request} that reactor-netty's
 * {@code MonoSendMany} issues from the netty event loop once 64 earlier buffers have been written. {@code FluxSubscribeOn} runs every such
 * request as its own task on the {@code repository_azure} pool, and {@link AzureClientProvider} backs that scheduler with a pool whose
 * workers do not serialize tasks. When a refill's task waits in the pool queue while netty drains the buffers already in flight, the next
 * refill is issued too, and once threads free up both requests run concurrently. Inside {@code FluxConcatMap} the pending buffer's
 * one-shot flag is a plain field, so the two requests can emit the same buffer twice and complete the inner twice, which makes the drain
 * skip the next buffer.
 * <p>
 * The test forces that queueing: the upload stream, which is under the test's control, blocks both pool threads for a moment at the end of
 * every burst. Netty keeps completing writes meanwhile, the two following refills queue up, and releasing the blockers starts them
 * together. Each stored blob is compared 64KB chunk by 64KB chunk with the source; the test fails on the first blob that holds a chunk twice.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureBlobStoreConcurrentRefillCorruptionTests extends AbstractAzureServerTestCase {

    // AzureBlobStore.DEFAULT_UPLOAD_BUFFERS_SIZE
    private static final int BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(64);
    // reactor.netty.channel.MonoSend.REFILL_SIZE: buffers produced per refill request
    private static final int REFILL_SIZE = 64;
    // about the size of the corrupted BCC: 643 buffers, so about ten refills per upload
    private static final int BLOB_SIZE = ByteSizeUnit.MB.toIntBytes(42);
    // before the fix a run like this stored a corrupted blob within a few dozen uploads
    private static final int MAX_UPLOADS = 100;
    // long enough for netty to drain all in-flight buffers to the local server while the pool is blocked
    private static final TimeValue BLOCK = TimeValue.timeValueMillis(40);

    @Override
    protected ByteSizeValue maxSinglePartUploadSize() {
        // the incident's node had a 31GB heap: parts of 100MB, so a 42MB BCC is one PUT Blob
        return ByteSizeValue.of(256, ByteSizeUnit.MB);
    }

    @Override
    protected Settings clientSettings() {
        return Settings.builder().put(AzureClientProvider.EVENT_LOOP_THREAD_COUNT.getKey(), 1).build();
    }

    @Override
    protected ExecutorBuilder<?> repositoryExecutorBuilder(Settings settings) {
        // two threads: both are held by the blockers, and both then pick up a queued refill at the same time
        return new ScalingExecutorBuilder(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME, 0, 2, TimeValue.timeValueSeconds(30L), false);
    }

    public void testConcurrentRefillsCorruptSinglePartUpload() throws Exception {
        final var handler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        httpServer.createContext("/", handler);
        final BlobContainer blobContainer = builder().withMaxRetries(2).withTryTimeout(TimeValue.timeValueSeconds(30)).build();
        final Executor pool = threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME);

        final byte[] data = randomByteArrayOfLength(BLOB_SIZE);
        final var streamsOpened = new AtomicInteger();
        final var pairings = new AtomicInteger();
        final var failedUploads = new ArrayList<String>();

        for (int upload = 0; upload < MAX_UPLOADS; upload++) {
            final String blobName = "refill_race_" + upload;
            try {
                blobContainer.writeBlobAtomic(randomPurpose(), blobName, BLOB_SIZE, (offset, length) -> {
                    streamsOpened.incrementAndGet();
                    return new BurstBlockingInputStream(data, Math.toIntExact(offset), Math.toIntExact(length), pool, pairings);
                }, false, Runnable::run);
            } catch (IOException e) {
                // a dropped buffer that is not paired with a duplicate leaves the request one buffer short: the SDK times out and retries
                failedUploads.add(blobName + ": " + e.getCause());
                continue;
            }
            final BytesReference committed = handler.getMockBlobStore().getBlob(blobName, null).getContents();
            assertNotNull(blobName, committed);
            assertEquals(blobName, BLOB_SIZE, committed.length());
            final String corruption = describeCorruption(blobName, data, BytesReference.toBytes(committed));
            if (corruption != null) {
                fail(
                    corruption
                        + "\nafter "
                        + (upload + 1)
                        + " uploads, "
                        + pairings.get()
                        + " forced refill pairings, "
                        + streamsOpened.get()
                        + " streams opened, "
                        + failedUploads.size()
                        + " uploads failed: "
                        + failedUploads
                );
            }
            blobContainer.deleteBlobsIgnoringIfNotExists(randomPurpose(), List.of(blobName).iterator());
            if ((upload + 1) % 10 == 0) {
                logger.info(
                    "--> {} uploads verified, {} forced refill pairings, {} streams opened, {} uploads failed",
                    upload + 1,
                    pairings.get(),
                    streamsOpened.get(),
                    failedUploads.size()
                );
            }
        }
        logger.info(
            "--> no corruption in {} uploads ({} forced refill pairings, {} streams opened, {} uploads failed: {})",
            MAX_UPLOADS,
            pairings.get(),
            streamsOpened.get(),
            failedUploads.size(),
            failedUploads
        );
    }

    /**
     * Lists every 64KB chunk of the committed blob that differs from the source and, for each, the source chunk it holds instead. Returns
     * {@code null} when the blob equals the source.
     */
    @Nullable
    private static String describeCorruption(String blobName, byte[] data, byte[] committedBytes) {
        StringBuilder description = null;
        final int chunks = (data.length + BUFFER_SIZE - 1) / BUFFER_SIZE;
        for (int chunk = 0; chunk < chunks; chunk++) {
            final int from = chunk * BUFFER_SIZE;
            final int to = Math.min(from + BUFFER_SIZE, data.length);
            if (Arrays.equals(data, from, to, committedBytes, from, to)) {
                continue;
            }
            if (description == null) {
                description = new StringBuilder(blobName).append(" differs from the source:");
            }
            description.append("\n  chunk ").append(chunk).append(" [").append(from).append(", ").append(to).append(')');
            final int length = to - from;
            int source = -1;
            for (int otherFrom = 0; otherFrom + length <= data.length; otherFrom += BUFFER_SIZE) {
                if (Arrays.equals(committedBytes, from, to, data, otherFrom, otherFrom + length)) {
                    source = otherFrom / BUFFER_SIZE;
                    break;
                }
            }
            if (source < 0) {
                description.append(" matches no source chunk");
            } else {
                description.append(" holds source chunk ").append(source).append(" (").append(source - chunk).append(')');
            }
        }
        return description == null ? null : description.toString();
    }

    /**
     * Serves the blob bytes and, whenever a read completes a burst of {@link #REFILL_SIZE} buffers, holds both pool threads for
     * {@link #BLOCK} so that the refills netty issues meanwhile queue up and start together once the threads are released.
     */
    private final class BurstBlockingInputStream extends InputStream {
        private final byte[] data;
        private final int end;
        private final Executor pool;
        private final AtomicInteger pairings;
        private int position;
        private int mark;

        BurstBlockingInputStream(byte[] data, int offset, int length, Executor pool, AtomicInteger pairings) {
            this.data = data;
            this.position = offset;
            this.mark = offset;
            this.end = offset + length;
            this.pool = pool;
            this.pairings = pairings;
        }

        // mark/reset are what the pre-#159365 upload path (convertStreamToByteBuffer) requires of its stream
        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public void mark(int readlimit) {
            mark = position;
        }

        @Override
        public void reset() {
            position = mark;
        }

        @Override
        public int read() {
            return position < end ? data[position++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (position >= end) {
                return -1;
            }
            final int n = Math.min(len, end - position);
            System.arraycopy(data, position, b, off, n);
            final int before = position;
            position += n;
            if (position / (REFILL_SIZE * BUFFER_SIZE) > before / (REFILL_SIZE * BUFFER_SIZE)) {
                blockPool();
            }
            return n;
        }

        private void blockPool() {
            pairings.incrementAndGet();
            final CountDownLatch release = new CountDownLatch(1);
            for (int i = 0; i < 2; i++) {
                pool.execute(() -> {
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            threadPool.schedule(release::countDown, BLOCK, threadPool.generic());
        }
    }
}
