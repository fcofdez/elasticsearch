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
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;

/**
 * Multipart uploads in {@link AzureBlobStore#writeBlobAtomic} turn every part into a {@code Flux} of 64KB buffers that is consumed by
 * reactor-netty's {@code MonoSendMany}. That subscriber requests 128 buffers when it subscribes and 64 more from the netty event loop
 * each time its write queue drains, so the part's {@link InputStream} is read in bursts whose scheduling depends on those refills. The
 * parts uploaded here are large enough to cross several refill boundaries. The thread reading each 64KB chunk is recorded with plain
 * writes only, so the recording does not add synchronization to the read path, and the committed blob is compared chunk by chunk with
 * the source bytes. The second test wraps the stream in the {@code synchronized} reads used by
 * {@code AzureBlobStore#convertStreamToByteBuffer} for comparison.
 */
public class AzureBlobStoreMultipartUploadReadThreadsTests extends AbstractAzureServerTestCase {

    // AzureBlobStore.DEFAULT_UPLOAD_BUFFERS_SIZE
    private static final int BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(64);
    // 256 buffers per part: one initial MonoSendMany batch of 128 followed by refills of 64
    private static final long UPLOAD_BLOCK_SIZE = ByteSizeUnit.MB.toBytes(16);

    @Override
    protected long uploadBlockSize() {
        return UPLOAD_BLOCK_SIZE;
    }

    public void testMultipartUploadReadThreads() throws Exception {
        runMultipartUpload(false);
    }

    public void testMultipartUploadReadThreadsWithSynchronizedReads() throws Exception {
        runMultipartUpload(true);
    }

    private void runMultipartUpload(boolean synchronizedReads) throws Exception {
        final var handler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        httpServer.createContext("/", handler);
        final BlobContainer blobContainer = createBlobContainer(randomIntBetween(0, 3));

        final int blobSize = Math.toIntExact(2 * UPLOAD_BLOCK_SIZE + randomIntBetween(1, ByteSizeUnit.MB.toIntBytes(1)));
        final byte[] data = randomByteArrayOfLength(blobSize);
        final PartReads[] parts = new PartReads[Math.toIntExact((blobSize + UPLOAD_BLOCK_SIZE - 1) / UPLOAD_BLOCK_SIZE)];
        for (int p = 0; p < parts.length; p++) {
            final long offset = p * UPLOAD_BLOCK_SIZE;
            final long length = Math.min(UPLOAD_BLOCK_SIZE, blobSize - offset);
            parts[p] = new PartReads(p, offset, length, randomSliceLengths(length));
        }

        final String blobName = "multipart_read_threads";
        blobContainer.writeBlobAtomic(randomPurpose(), blobName, blobSize, (offset, length) -> {
            final PartReads part = parts[Math.toIntExact(offset / UPLOAD_BLOCK_SIZE)];
            assertEquals(part.offset, offset);
            assertEquals(part.length, length);
            final InputStream stream = part.open(data);
            return synchronizedReads ? withSynchronizedReads(stream) : stream;
        }, randomBoolean(), Runnable::run);

        final BytesReference committed = handler.getMockBlobStore().getBlob(blobName, null).getContents();
        assertNotNull(committed);
        assertEquals(blobSize, committed.length());
        final byte[] committedBytes = BytesReference.toBytes(committed);

        for (PartReads part : parts) {
            assertEquals("part " + part.part + " stream opened more than once", 1, part.streamsOpened.get());
            assertEquals("part " + part.part + " has chunks read by more than one thread", 0, part.concurrentChunkReads.get());
            assertEquals("part " + part.part + " bytes read", part.length, part.bytesReadAtClose);

            final Set<String> readers = new LinkedHashSet<>();
            final List<Integer> readerChanges = new ArrayList<>();
            for (int chunk = 0; chunk < part.chunks(); chunk++) {
                final Thread reader = part.readers.get(chunk);
                assertNotNull("part " + part.part + " chunk " + chunk + " was not read", reader);
                assertEquals(reader.getName(), AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME, EsExecutors.executorName(reader));
                readers.add(reader.getName());
                if (chunk > 0 && reader != part.readers.get(chunk - 1)) {
                    readerChanges.add(chunk);
                }
                final int from = Math.toIntExact(part.offset + (long) chunk * BUFFER_SIZE);
                final int to = Math.toIntExact(Math.min(from + BUFFER_SIZE, part.offset + part.length));
                if (Arrays.equals(data, from, to, committedBytes, from, to) == false) {
                    fail(describeCorruption(part, chunk, data, committedBytes));
                }
            }
            logger.info(
                "--> part [{}] ({} chunks) read by {} thread(s) {}, reader changed at chunks {}",
                part.part,
                part.chunks(),
                readers.size(),
                readers,
                readerChanges
            );
        }
    }

    private static String describeCorruption(PartReads part, int chunk, byte[] data, byte[] committedBytes) {
        final int from = Math.toIntExact(part.offset + (long) chunk * BUFFER_SIZE);
        final int to = Math.toIntExact(Math.min(from + BUFFER_SIZE, part.offset + part.length));
        final var description = new StringBuilder().append("part ")
            .append(part.part)
            .append(" chunk ")
            .append(chunk)
            .append(" (blob offset ")
            .append(from)
            .append(", read by ")
            .append(part.readers.get(chunk))
            .append(") differs from the source");
        for (int other = 0; other < part.chunks(); other++) {
            final int otherFrom = Math.toIntExact(part.offset + (long) other * BUFFER_SIZE);
            final int otherTo = otherFrom + (to - from);
            if (otherTo <= part.offset + part.length && Arrays.equals(committedBytes, from, to, data, otherFrom, otherTo)) {
                description.append("; it equals source chunk ")
                    .append(other)
                    .append(" (")
                    .append(chunk - other)
                    .append(" chunks earlier, read by ")
                    .append(part.readers.get(other))
                    .append(')');
                break;
            }
        }
        return description.toString();
    }

    private static long[] randomSliceLengths(long length) {
        final int slices = randomIntBetween(1, 8);
        final long[] lengths = new long[slices];
        long remaining = length;
        for (int i = 0; i < slices - 1; i++) {
            lengths[i] = randomLongBetween(1, remaining - (slices - 1 - i));
            remaining -= lengths[i];
        }
        lengths[slices - 1] = remaining;
        return lengths;
    }

    private static InputStream withSynchronizedReads(InputStream delegate) {
        return new FilterInputStream(delegate) {
            @Override
            public synchronized int read(byte[] b, int off, int len) throws IOException {
                return super.read(b, off, len);
            }

            @Override
            public synchronized int read() throws IOException {
                return super.read();
            }
        };
    }

    private static final class PartReads {
        private final int part;
        private final long offset;
        private final long length;
        private final long[] sliceLengths;
        private final AtomicReferenceArray<Thread> readers;
        private final AtomicInteger streamsOpened = new AtomicInteger();
        private final AtomicInteger concurrentChunkReads = new AtomicInteger();
        private volatile long bytesReadAtClose = -1L;

        PartReads(int part, long offset, long length, long[] sliceLengths) {
            this.part = part;
            this.offset = offset;
            this.length = length;
            this.sliceLengths = sliceLengths;
            this.readers = new AtomicReferenceArray<>(Math.toIntExact((length + BUFFER_SIZE - 1) / BUFFER_SIZE));
        }

        int chunks() {
            return readers.length();
        }

        InputStream open(byte[] data) {
            streamsOpened.incrementAndGet();
            return new RecordingInputStream(new SlicedInputStream(sliceLengths.length) {
                @Override
                protected InputStream openSlice(int slice) {
                    long start = offset;
                    for (int i = 0; i < slice; i++) {
                        start += sliceLengths[i];
                    }
                    final int sliceLength = Math.toIntExact(sliceLengths[slice]);
                    return new InputStreamIndexInput(
                        new ByteArrayIndexInput("part-" + part, data, Math.toIntExact(start), sliceLength),
                        sliceLength
                    );
                }
            });
        }

        private class RecordingInputStream extends FilterInputStream {
            private long position;

            RecordingInputStream(InputStream in) {
                super(in);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                record();
                final int read = super.read(b, off, len);
                if (read > 0) {
                    position += read;
                }
                return read;
            }

            @Override
            public int read() throws IOException {
                record();
                final int read = super.read();
                if (read >= 0) {
                    position++;
                }
                return read;
            }

            @Override
            public void close() throws IOException {
                bytesReadAtClose = position;
                super.close();
            }

            private void record() {
                final int chunk = Math.toIntExact(position / BUFFER_SIZE);
                if (chunk >= readers.length()) {
                    return;
                }
                final Thread current = Thread.currentThread();
                if (readers.compareAndSet(chunk, null, current) == false && readers.get(chunk) != current) {
                    concurrentChunkReads.incrementAndGet();
                }
            }
        }
    }
}
