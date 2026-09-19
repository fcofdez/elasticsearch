/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.azure.AzureRepositoryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.zip.CRC32;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Reproduces the BCC corruption of incident stateless_commit_3431 with a real indexing node uploading real BCCs to a mock Azure server: a
 * stored BCC in which one 64KB upload buffer appears twice and the following one is missing, with the blob length intact.
 * <p>
 * The Azure upload {@code Flux} in {@code AzureBlobStore#toFlux} reads the BCC stream in bursts of 64 buffers, each burst produced by one
 * {@code request} that reactor-netty issues from the netty event loop once 64 earlier buffers have been written. {@code FluxSubscribeOn}
 * runs every such request as its own task on the {@code repository_azure} pool, and {@code AzureClientProvider} backs that scheduler with a
 * pool whose workers do not serialize tasks. When a refill's task waits in the pool queue while netty drains the buffers already in flight,
 * the next refill is issued too, and once threads free up both requests run concurrently. Inside {@code FluxConcatMap} the pending buffer's
 * one-shot flag is a plain field, so the two requests can emit the same buffer twice and complete the inner twice, which makes the drain
 * skip the next buffer. The upload's own byte count only counts stream reads, so it stays right and the SDK reports success.
 * <p>
 * A busy indexing node queues refills like that on its own. This test does it from outside: while the index is being flushed repeatedly,
 * a thread keeps occupying every {@code repository_azure} thread for a moment and releasing them together. After each flush every new BCC
 * in the mock store is scanned for a non-zero 64KB-aligned chunk that appears twice, which does not happen in a well-formed BCC.
 */
@SuppressForbidden(reason = "uses HttpServer to emulate Azure storage")
public class AzureBccUploadRefillRaceIT extends AbstractStatelessPluginIntegTestCase {

    private static final String ACCOUNT = "account";
    private static final String CONTAINER = "container";

    // AzureBlobStore.DEFAULT_UPLOAD_BUFFERS_SIZE
    private static final int BUFFER_SIZE = 64 * 1024;
    // before the fix the ninth flush of a run like this uploaded a corrupted BCC
    private static final int ROUNDS = 30;
    private static final int ROUNDS_PER_INDEX = 5;
    private static final int DOCS_PER_ROUND = 300;
    private static final int DOC_SIZE = 48 * 1024;
    // how long the pool is held: long enough for netty to drain the in-flight buffers of an upload to the local server
    private static final long SQUEEZE_MILLIS = 30;
    private static final long GAP_MILLIS = 20;

    private static TestObjectStoreServer testServer;
    private static AzureHttpHandler azureHandler;

    @BeforeClass
    public static void startServer() throws IOException {
        testServer = new TestObjectStoreServer();
        azureHandler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        testServer.start();
        testServer.setUp(Map.of("/" + ACCOUNT, azureHandler));
    }

    @AfterClass
    public static void stopServer() {
        if (testServer != null) {
            testServer.tearDown();
            testServer.stop();
            testServer = null;
        }
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(AzureRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.test.account", ACCOUNT);
        // The mock server does not validate HMAC signatures; any base64-encoded value works.
        secureSettings.setString("azure.client.test.key", Base64.getEncoder().encodeToString("test-key".getBytes(StandardCharsets.UTF_8)));

        String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://" + testServer.serverUrl() + "/" + ACCOUNT;

        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // upload every commit as its own BCC so each flush is one upload
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.AZURE)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), CONTAINER)
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test")
            .put("azure.client.test.endpoint_suffix", endpoint)
            .put("azure.client.test.max_retries", 2)
            .setSecureSettings(secureSettings);
    }

    public void testConcurrentRefillsCorruptUploadedBcc() throws Exception {
        final String indexNode = startMasterAndIndexNode();
        String indexName = randomIndexName();
        createIndex(indexName, noMergesIndexSettings());

        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        final PoolSqueezer squeezer = new PoolSqueezer(threadPool);
        squeezer.start();
        try {
            final Set<String> scanned = new HashSet<>();
            int blobsScanned = 0;
            for (int round = 0; round < ROUNDS; round++) {
                if (round > 0 && round % ROUNDS_PER_INDEX == 0) {
                    // the mock store shares the test JVM's heap with the node and keeps every live BCC: start over with a fresh index and
                    // drop the old one's blobs
                    final String indexUUID = resolveIndex(indexName).getUUID();
                    assertAcked(indicesAdmin().prepareDelete(indexName));
                    final MockAzureBlobStore blobStore = azureHandler.getMockBlobStore();
                    for (String path : blobStore.listBlobs("indices/" + indexUUID + "/", null).keySet()) {
                        try {
                            blobStore.deleteBlob(path, null);
                        } catch (MockAzureBlobStore.AzureBlobStoreError e) {
                            // already deleted by the node
                        }
                    }
                    indexName = randomIndexName();
                    createIndex(indexName, noMergesIndexSettings());
                }
                // random alphanumerics do not compress well, so each flush uploads a BCC of several hundred 64KB buffers
                indexDocs(
                    indexName,
                    DOCS_PER_ROUND,
                    UnaryOperator.identity(),
                    null,
                    () -> Map.of("data", randomAlphanumericOfLength(DOC_SIZE))
                );
                flush(indexName);
                for (Map.Entry<String, BytesReference> blob : azureHandler.blobs().entrySet()) {
                    if (blob.getKey().contains(StatelessCompoundCommit.PREFIX) == false || scanned.add(blob.getKey()) == false) {
                        continue;
                    }
                    blobsScanned++;
                    final String corruption = describeDuplicateChunks(blob.getKey(), blob.getValue());
                    if (corruption != null) {
                        fail(
                            corruption
                                + "\nafter "
                                + (round + 1)
                                + " flushes, "
                                + blobsScanned
                                + " BCC blobs scanned, "
                                + squeezer.squeezes.get()
                                + " pool squeezes"
                        );
                    }
                }
                if ((round + 1) % 10 == 0) {
                    logger.info("--> {} flushes, {} BCC blobs scanned, {} pool squeezes", round + 1, blobsScanned, squeezer.squeezes.get());
                }
            }
            logger.info(
                "--> no corruption after {} flushes, {} BCC blobs scanned, {} pool squeezes",
                ROUNDS,
                blobsScanned,
                squeezer.squeezes.get()
            );
        } finally {
            squeezer.stop();
        }
    }

    /**
     * One shard, and segments larger than half a megabyte are never merged: a merge would re-upload the merged data in the next BCC, which
     * grows the blobs the mock store has to keep with every flush.
     */
    private static Settings noMergesIndexSettings() {
        return indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING.getKey(), "1mb").build();
    }

    /**
     * Finds 64KB-aligned chunks that appear twice in a BCC blob, ignoring all-zero padding, reading the blob through one reusable buffer
     * rather than copying it. Returns {@code null} when there are none.
     */
    private static String describeDuplicateChunks(String blobName, BytesReference blob) throws IOException {
        final Map<Long, Integer> firstChunkByCrc = new HashMap<>();
        final byte[] bytes = new byte[BUFFER_SIZE];
        StringBuilder description = null;
        try (StreamInput in = blob.streamInput()) {
            for (int chunk = 0; (chunk + 1) * BUFFER_SIZE <= blob.length(); chunk++) {
                in.readBytes(bytes, 0, BUFFER_SIZE);
                if (allZero(bytes)) {
                    continue;
                }
                final CRC32 crc = new CRC32();
                crc.update(bytes, 0, BUFFER_SIZE);
                final Integer earlier = firstChunkByCrc.putIfAbsent(crc.getValue(), chunk);
                if (earlier != null
                    && blob.slice(earlier * BUFFER_SIZE, BUFFER_SIZE).equals(blob.slice(chunk * BUFFER_SIZE, BUFFER_SIZE))) {
                    if (description == null) {
                        description = new StringBuilder(blobName).append(" (")
                            .append(blob.length())
                            .append(" bytes) holds a 64KB upload buffer twice:");
                    }
                    description.append("\n  chunk ")
                        .append(chunk)
                        .append(" [")
                        .append(chunk * BUFFER_SIZE)
                        .append(", ")
                        .append((chunk + 1) * BUFFER_SIZE)
                        .append(") == chunk ")
                        .append(earlier)
                        .append(" (")
                        .append(chunk - earlier)
                        .append(" chunks earlier)");
                }
            }
        }
        return description == null ? null : description.toString();
    }

    private static boolean allZero(byte[] b) {
        for (byte value : b) {
            if (value != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Occupies every {@code repository_azure} thread of the index node for {@link #SQUEEZE_MILLIS}, releases them together, waits
     * {@link #GAP_MILLIS} and repeats. Refills that reactor-netty issues while the threads are held queue up behind the blockers and start
     * concurrently once they are released.
     */
    private static final class PoolSqueezer implements Runnable {
        private final Executor pool;
        private final int threads;
        private final Thread thread = new Thread(this, "repository-azure-pool-squeezer");
        private final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger squeezes = new AtomicInteger();

        PoolSqueezer(ThreadPool threadPool) {
            this.pool = threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME);
            this.threads = threadPool.info(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME).getMax();
        }

        void start() {
            thread.start();
        }

        void stop() throws InterruptedException {
            running.set(false);
            thread.join(TimeValue.timeValueSeconds(10).millis());
        }

        @Override
        public void run() {
            while (running.get()) {
                final CountDownLatch release = new CountDownLatch(1);
                for (int i = 0; i < threads; i++) {
                    pool.execute(() -> {
                        try {
                            release.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
                squeezes.incrementAndGet();
                try {
                    Thread.sleep(SQUEEZE_MILLIS);
                    release.countDown();
                    Thread.sleep(GAP_MILLIS);
                } catch (InterruptedException e) {
                    release.countDown();
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
