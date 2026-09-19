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
import io.netty.util.Version;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpPrincipal;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.fixtures.tls.TestTlsCertificate;
import org.elasticsearch.test.fixtures.tls.TestTrustStore;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;

/**
 * Stresses the upload paths of {@link AzureBlobStore} the way a busy indexing node does: a two-thread {@code repository_azure} pool
 * shared by several concurrent uploads, a single netty event loop, a server that drains request bodies slowly and in irregular pieces so
 * that the channel keeps toggling writability, injected 5xx responses and connection drops that make the SDK re-subscribe to the body
 * while the previous attempt is still winding down, and jittered stream reads. Each blob is large enough to cross several
 * {@code MonoSendMany} refill boundaries. The committed blobs are compared 64KB chunk by 64KB chunk with the source and, on a mismatch,
 * every differing chunk is matched against all source chunks so that the shape of the corruption (a buffer sent twice, dropped or
 * shifted) is visible in the failure message.
 * <p>
 * Three paths are covered: {@link AzureBlobStore#writeBlobAtomic} as one {@code PUT Blob}, which is how a BCC below the multipart
 * threshold is uploaded, the same as a multipart upload, and the {@link InputStream} based
 * {@link BlobContainer#writeBlob(org.elasticsearch.common.blobstore.OperationPurpose, String, InputStream, long, boolean)}, which still
 * goes through {@code convertStreamToByteBuffer} and its mark/reset on a stream shared between retries. Each path runs over plain HTTP
 * and over TLS, where netty's {@code SslHandler} re-slices every 64KB buffer into 16KB records and interleaves wrapping with flushes.
 */
@SuppressForbidden(reason = "uses an http(s) server and installs the test certificate into the JVM default trust store")
public class AzureBlobStoreMultipartUploadStressTests extends AbstractAzureServerTestCase {

    private static final TestTlsCertificate TLS_CERTIFICATE = TestTlsCertificate.generate("localhost");

    @ClassRule(order = 1)
    public static final TestTrustStore TRUST_STORE = new TestTrustStore(TLS_CERTIFICATE::getPemCertificateStream);

    private static String previousTrustStore;
    private static String previousTrustStoreType;

    /**
     * The SDK's netty client validates the server certificate against the JVM default trust store and {@link AzureClientProvider} exposes
     * no way to configure it. reactor-netty builds its default client {@code SslContext} once per JVM on first use, so this must run
     * before the first TLS request of the JVM.
     */
    @BeforeClass
    public static void trustTestCertificate() {
        previousTrustStore = System.getProperty("javax.net.ssl.trustStore");
        previousTrustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
        System.setProperty("javax.net.ssl.trustStore", TRUST_STORE.getTrustStorePath().toString());
        System.setProperty("javax.net.ssl.trustStoreType", TRUST_STORE.getTrustStoreType());
    }

    @AfterClass
    public static void restoreTrustStore() {
        restoreProperty("javax.net.ssl.trustStore", previousTrustStore);
        restoreProperty("javax.net.ssl.trustStoreType", previousTrustStoreType);
    }

    private static void restoreProperty(String key, @Nullable String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }

    // AzureBlobStore.DEFAULT_UPLOAD_BUFFERS_SIZE
    private static final int BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(64);
    // 256 buffers per part: one initial MonoSendMany batch of 128 followed by refills of 64
    private static final long UPLOAD_BLOCK_SIZE = ByteSizeUnit.MB.toBytes(16);
    private static final int CONCURRENT_UPLOADS = 4;
    private static final int ROUNDS = 2;
    // one in N uploads gets a 5xx, a dropped connection or a stall past the client's try timeout
    private static final int FAILURE_ONE_IN = 16;
    // the SDK cancels an attempt that has not completed within this time and retries it on a fresh connection. Generous enough that a
    // throttled but healthy upload always completes within it: the mock server fails the test if a client closes a healthy upload early.
    private static final TimeValue TRY_TIMEOUT = TimeValue.timeValueSeconds(20);
    // a stalled upload stops reading the body for this long, so the client's try timeout fires while the body is still being sent
    private static final TimeValue STALL = TimeValue.timeValueSeconds(25);

    private enum Mode {
        /** {@link AzureBlobStore#writeBlobAtomic} below the single part threshold: one {@code PUT Blob} per blob. */
        SINGLE_ATOMIC,
        /** {@link AzureBlobStore#writeBlobAtomic} above the single part threshold: one {@code PUT Block} per part. */
        MULTIPART_ATOMIC,
        /** {@link BlobContainer#writeBlob} with an {@link InputStream} below the single part threshold: one {@code PUT Blob} per blob. */
        SINGLE_STREAM
    }

    private volatile ByteSizeValue maxSinglePartUploadSize = ByteSizeValue.of(1, ByteSizeUnit.MB);

    @Override
    protected long uploadBlockSize() {
        return UPLOAD_BLOCK_SIZE;
    }

    @Override
    protected ByteSizeValue maxSinglePartUploadSize() {
        return maxSinglePartUploadSize;
    }

    @Override
    protected Settings clientSettings() {
        return Settings.builder().put(AzureClientProvider.EVENT_LOOP_THREAD_COUNT.getKey(), 1).build();
    }

    @Override
    protected ExecutorBuilder<?> repositoryExecutorBuilder(Settings settings) {
        return new ScalingExecutorBuilder(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME, 0, 2, TimeValue.timeValueSeconds(30L), false);
    }

    @Override
    protected String getEndpointForServer(HttpServer server, String accountName) {
        if (server instanceof HttpsServer) {
            return "https://localhost:" + server.getAddress().getPort() + "/" + accountName;
        }
        return super.getEndpointForServer(server, accountName);
    }

    /**
     * Replaces the server started by {@link #initServer()}, which runs handlers one at a time on its dispatcher thread, with one that runs
     * them on the generic pool so that a stalled upload does not hold up the healthy ones, optionally serving {@link #TLS_CERTIFICATE}.
     */
    private void restartServer(boolean tls) throws Exception {
        httpServer.stop(0);
        final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        final HttpServer server;
        if (tls) {
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(
                new KeyManager[] {
                    KeyStoreUtil.createKeyManager(
                        new Certificate[] { TLS_CERTIFICATE.certificate() },
                        TLS_CERTIFICATE.privateKey(),
                        null
                    ) },
                null,
                null
            );
            final HttpsServer httpsServer = HttpsServer.create(address, 0);
            httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
            server = httpsServer;
        } else {
            server = HttpServer.create(address, 0);
        }
        server.setExecutor(threadPool.generic());
        server.start();
        httpServer = server;
    }

    public void testConcurrentSingleUploadsUnderStress() throws Exception {
        runUploads(Mode.SINGLE_ATOMIC, false);
    }

    public void testConcurrentMultipartUploadsUnderStress() throws Exception {
        runUploads(Mode.MULTIPART_ATOMIC, false);
    }

    public void testConcurrentSingleStreamUploadsUnderStress() throws Exception {
        runUploads(Mode.SINGLE_STREAM, false);
    }

    public void testConcurrentSingleUploadsUnderStressOverTls() throws Exception {
        runUploads(Mode.SINGLE_ATOMIC, true);
    }

    public void testConcurrentMultipartUploadsUnderStressOverTls() throws Exception {
        runUploads(Mode.MULTIPART_ATOMIC, true);
    }

    public void testConcurrentSingleStreamUploadsUnderStressOverTls() throws Exception {
        runUploads(Mode.SINGLE_STREAM, true);
    }

    private void runUploads(Mode mode, boolean tls) throws Exception {
        restartServer(tls);
        final long seed = randomLong();
        final var handler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        final var injectedFailures = new AtomicInteger();
        final var injectedStalls = new AtomicInteger();
        final var uploads = new AtomicInteger();
        httpServer.createContext("/", new StressingHandler(handler, new Random(seed), injectedFailures, injectedStalls, uploads));
        maxSinglePartUploadSize = mode == Mode.MULTIPART_ATOMIC
            ? ByteSizeValue.of(1, ByteSizeUnit.MB)
            : ByteSizeValue.of(256, ByteSizeUnit.MB);
        final BlobContainer blobContainer = builder().withMaxRetries(randomIntBetween(4, 6)).withTryTimeout(TRY_TIMEOUT).build();

        final int blobSize = Math.toIntExact(2 * UPLOAD_BLOCK_SIZE + randomIntBetween(1, ByteSizeUnit.MB.toIntBytes(1)));
        final byte[] data = randomByteArrayOfLength(blobSize);
        final var streamsOpened = new AtomicInteger();
        final var concurrentReads = new AtomicInteger();
        final BlobContainer.BlobMultiPartInputStreamProvider provider = (offset, length) -> {
            final int attempt = streamsOpened.incrementAndGet();
            return new JitteredInputStream(
                slicedStream(data, offset, length, new Random(seed ^ offset ^ ((long) attempt << 40))),
                new Random(seed + attempt),
                concurrentReads
            );
        };

        for (int round = 0; round < ROUNDS; round++) {
            final List<String> blobNames = new ArrayList<>();
            for (int i = 0; i < CONCURRENT_UPLOADS; i++) {
                blobNames.add("stress_" + mode + (tls ? "_tls_" : "_") + round + "_" + i);
            }
            startInParallel(CONCURRENT_UPLOADS, i -> {
                try {
                    if (mode == Mode.SINGLE_STREAM) {
                        try (InputStream stream = provider.apply(0L, blobSize)) {
                            blobContainer.writeBlob(randomPurpose(), blobNames.get(i), stream, blobSize, false);
                        }
                    } else {
                        blobContainer.writeBlobAtomic(randomPurpose(), blobNames.get(i), blobSize, provider, false, Runnable::run);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            for (String blobName : blobNames) {
                final BytesReference committed = handler.getMockBlobStore().getBlob(blobName, null).getContents();
                assertNotNull(blobName, committed);
                assertEquals(blobName, blobSize, committed.length());
                final String corruption = describeCorruption(blobName, data, BytesReference.toBytes(committed));
                if (corruption != null) {
                    fail(corruption);
                }
            }
            assertEquals(0, concurrentReads.get());
            blobContainer.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobNames.iterator());
            logger.info(
                "--> [{}][{}][{}] round [{}] verified {} blobs, {} uploads so far ({} failures injected of which {} stalls, {} streams opened)",
                mode,
                tls ? "https" : "http",
                Version.identify().get("netty-handler"),
                round,
                blobNames.size(),
                uploads.get(),
                injectedFailures.get(),
                injectedStalls.get(),
                streamsOpened.get()
            );
        }
    }

    private static InputStream slicedStream(byte[] data, long offset, long length, Random random) {
        final int slices = 1 + random.nextInt(8);
        final long[] sliceLengths = new long[slices];
        long remaining = length;
        for (int i = 0; i < slices - 1; i++) {
            sliceLengths[i] = 1 + (long) (random.nextDouble() * (remaining - (slices - 1 - i) - 1));
            remaining -= sliceLengths[i];
        }
        sliceLengths[slices - 1] = remaining;
        return new SlicedInputStream(slices) {
            @Override
            protected InputStream openSlice(int slice) {
                long start = offset;
                for (int i = 0; i < slice; i++) {
                    start += sliceLengths[i];
                }
                final int sliceLength = Math.toIntExact(sliceLengths[slice]);
                return new InputStreamIndexInput(new ByteArrayIndexInput("slice", data, Math.toIntExact(start), sliceLength), sliceLength);
            }
        };
    }

    /**
     * Lists every 64KB chunk of the committed blob that differs from the source and, for each, the source chunk it holds instead, if any.
     * Returns {@code null} when the blob equals the source.
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
     * Slows down and jitters reads of the upload stream, and detects overlapping reads from different threads.
     */
    private static final class JitteredInputStream extends FilterInputStream {
        private final Random random;
        private final AtomicInteger concurrentReads;
        private final AtomicLong readers = new AtomicLong();

        JitteredInputStream(InputStream in, Random random, AtomicInteger concurrentReads) {
            super(in);
            this.random = random;
            this.concurrentReads = concurrentReads;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (readers.incrementAndGet() != 1) {
                concurrentReads.incrementAndGet();
            }
            try {
                if (random.nextInt(1024) == 0) {
                    // long enough for an injected failure response to cancel the upload while this read is in flight
                    Thread.sleep(50 + random.nextInt(150));
                } else if (random.nextInt(64) == 0) {
                    Thread.sleep(random.nextInt(3));
                }
                return super.read(b, off, random.nextInt(4) == 0 ? 1 + random.nextInt(len) : len);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } finally {
                readers.decrementAndGet();
            }
        }

        @Override
        public int read() throws IOException {
            final byte[] b = new byte[1];
            final int n = read(b, 0, 1);
            return n <= 0 ? -1 : b[0] & 0xFF;
        }
    }

    /**
     * Drains upload bodies ({@code PUT Blob} and {@code PUT Block}) slowly and in irregular pieces, and fails one in
     * {@link #FAILURE_ONE_IN} of them with a 5xx or a dropped connection so that the SDK retries.
     */
    private static final class StressingHandler implements HttpHandler {
        private final HttpHandler delegate;
        private final Random random;
        private final AtomicInteger injectedFailures;
        private final AtomicInteger injectedStalls;
        private final AtomicInteger uploads;

        StressingHandler(
            HttpHandler delegate,
            Random random,
            AtomicInteger injectedFailures,
            AtomicInteger injectedStalls,
            AtomicInteger uploads
        ) {
            this.delegate = delegate;
            this.random = random;
            this.injectedFailures = injectedFailures;
            this.injectedStalls = injectedStalls;
            this.uploads = uploads;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("PUT".equals(exchange.getRequestMethod()) && isUpload(exchange.getRequestURI().getRawQuery())) {
                uploads.incrementAndGet();
                final boolean fail;
                final long exchangeSeed;
                synchronized (random) {
                    fail = random.nextInt(FAILURE_ONE_IN) == 0;
                    exchangeSeed = random.nextLong();
                }
                final Random exchangeRandom = new Random(exchangeSeed);
                if (fail) {
                    injectedFailures.incrementAndGet();
                    final long contentLength = Long.parseLong(exchange.getRequestHeaders().getFirst("Content-Length"));
                    readFromInputStream(exchange.getRequestBody(), (long) (exchangeRandom.nextDouble() * contentLength));
                    switch (exchangeRandom.nextInt(3)) {
                        case 0 -> AzureHttpHandler.sendError(
                            exchange,
                            exchangeRandom.nextBoolean() ? RestStatus.INTERNAL_SERVER_ERROR : RestStatus.SERVICE_UNAVAILABLE
                        );
                        case 1 -> {
                            // dropped connection: close without a response
                        }
                        case 2 -> {
                            // stall: keep the connection open without reading the rest of the body until the client has given up
                            injectedStalls.incrementAndGet();
                            try {
                                Thread.sleep(STALL.millis());
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        default -> throw new AssertionError();
                    }
                    exchange.close();
                    return;
                }
                delegate.handle(new ThrottledExchange(exchange, exchangeRandom));
            } else {
                delegate.handle(exchange);
            }
        }

        /** {@code PUT Blob} has no query string and {@code PUT Block} carries the block id; the block list commit and leases are left alone. */
        private static boolean isUpload(@Nullable String query) {
            return query == null || query.contains("blockid=");
        }
    }

    /**
     * Delegating {@link HttpExchange} whose request body is read in small random pieces with occasional pauses, so the client's
     * channel repeatedly fills up and drains.
     */
    private static final class ThrottledExchange extends HttpExchange {
        private final HttpExchange delegate;
        private final Random random;

        ThrottledExchange(HttpExchange delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public InputStream getRequestBody() {
            return new FilterInputStream(delegate.getRequestBody()) {
                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    if (random.nextInt(16) == 0) {
                        try {
                            Thread.sleep(random.nextInt(2));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                    }
                    return super.read(b, off, Math.min(len, 1 + random.nextInt(ByteSizeUnit.KB.toIntBytes(16))));
                }
            };
        }

        @Override
        public Headers getRequestHeaders() {
            return delegate.getRequestHeaders();
        }

        @Override
        public Headers getResponseHeaders() {
            return delegate.getResponseHeaders();
        }

        @Override
        public URI getRequestURI() {
            return delegate.getRequestURI();
        }

        @Override
        public String getRequestMethod() {
            return delegate.getRequestMethod();
        }

        @Override
        public HttpContext getHttpContext() {
            return delegate.getHttpContext();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public OutputStream getResponseBody() {
            return delegate.getResponseBody();
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) throws IOException {
            delegate.sendResponseHeaders(rCode, responseLength);
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return delegate.getRemoteAddress();
        }

        @Override
        public int getResponseCode() {
            return delegate.getResponseCode();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return delegate.getLocalAddress();
        }

        @Override
        public String getProtocol() {
            return delegate.getProtocol();
        }

        @Override
        public Object getAttribute(String name) {
            return delegate.getAttribute(name);
        }

        @Override
        public void setAttribute(String name, Object value) {
            delegate.setAttribute(name, value);
        }

        @Override
        public void setStreams(InputStream i, OutputStream o) {
            delegate.setStreams(i, o);
        }

        @Override
        public HttpPrincipal getPrincipal() {
            return delegate.getPrincipal();
        }
    }
}
