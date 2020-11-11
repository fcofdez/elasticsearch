/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AzureBlobStore implements BlobStore {

    private static final Logger logger = LogManager.getLogger(AzureBlobStore.class);

    private final AzureStorageService service;
    private final ThreadPool threadPool;

    private final String clientName;
    private final String container;
    private final LocationMode locationMode;

    private final Stats stats = new Stats();

    private final Consumer<HttpURLConnection> getMetricsCollector;
    private final Consumer<HttpURLConnection> listMetricsCollector;
    private final Consumer<HttpURLConnection> uploadMetricsCollector;

    public AzureBlobStore(RepositoryMetadata metadata, AzureStorageService service, ThreadPool threadPool) {
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.clientName = Repository.CLIENT_NAME.get(metadata.settings());
        this.service = service;
        this.threadPool = threadPool;
        // locationMode is set per repository, not per client
        this.locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
//        final Map<String, AzureStorageSettings> prevSettings = this.service.refreshAndClearCache(emptyMap());
//        final Map<String, AzureStorageSettings> newSettings = AzureStorageSettings.overrideLocationMode(prevSettings, locationMode);
//        this.service.refreshAndClearCache(newSettings);
        this.getMetricsCollector = (httpURLConnection) -> {
            if (httpURLConnection.getRequestMethod().equals("HEAD")) {
                stats.headOperations.incrementAndGet();
                return;
            }
            assert httpURLConnection.getRequestMethod().equals("GET");

            stats.getOperations.incrementAndGet();
        };
        this.listMetricsCollector = (httpURLConnection) -> {
            assert httpURLConnection.getRequestMethod().equals("GET");
            stats.listOperations.incrementAndGet();
        };
        this.uploadMetricsCollector = (httpURLConnection -> {
           assert httpURLConnection.getRequestMethod().equals("PUT");
            String queryParams = httpURLConnection.getURL().getQuery();
            if (queryParams != null && isBlockUpload(queryParams)) {
                stats.putBlockOperations.incrementAndGet();
            } else {
                stats.putOperations.incrementAndGet();
            }
        });
    }

    private boolean isBlockUpload(String queryParams) {
        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
        return (queryParams.contains("comp=block") && queryParams.contains("blockid="))
            || queryParams.contains("comp=blocklist");
    }

    @Override
    public String toString() {
        return container;
    }

    public AzureStorageService getService() {
        return service;
    }

    /**
     * Gets the configured {@link LocationMode} for the Azure storage requests.
     */
    public LocationMode getLocationMode() {
        return locationMode;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(path, this);
    }

    @Override
    public void close() {
    }

    public boolean blobExists(String blob) {
        // Container name must be lower case.
        //final OperationContext context = hookMetricCollector(client.v2().get(), getMetricsCollector);
        try(final AzureBlobServiceClientRef blobServiceClient = client()) {
            final BlobServiceClient client = blobServiceClient.getClient();

            Boolean blobExists = SocketAccess.doPrivilegedException(() -> {
                final BlobClient azureBlob = client.getBlobContainerClient(container).getBlobClient(blob);
                return azureBlob.exists();
            });
            return blobExists != null ? blobExists : false;
        }
    }

    public void deleteBlob(String blob) throws URISyntaxException {
        //final OperationContext context = hookMetricCollector(client.v2().get(), getMetricsCollector);
        try(final AzureBlobServiceClientRef blobServiceClient = client()) {
            final BlobServiceClient client = blobServiceClient.getClient();
            logger.trace(() -> new ParameterizedMessage("delete blob for container [{}], blob [{}]", container, blob));
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobContainerClient blobContainer = client.getBlobContainerClient(container);
                final BlobClient azureBlob = blobContainer.getBlobClient(blob);
                logger.trace(() -> new ParameterizedMessage("container [{}]: blob [{}] found. removing.", container, blob));
                azureBlob.delete();
//            azureBlob.delete(DeleteSnapshotsOption.NONE, null, null, client.v2().get());
            });
        }
    }

    public DeleteResult deleteBlobDirectory(String path) {
        final AtomicInteger blobsDeleted = new AtomicInteger(0);
        final AtomicLong bytesDeleted = new AtomicLong(0);

        try {
            try(final AzureBlobServiceClientRef blobServiceClient = client()) {
                final BlobServiceClient client = blobServiceClient.getClient();
                SocketAccess.doPrivilegedVoidException(() -> {
                    final BlobContainerClient blobContainerClient = client.getBlobContainerClient(container);
                    final List<String> blobURLs = new ArrayList<>();
                    final Queue<String> directories = new ArrayDeque<>();
                    directories.offer(path);
                    String dir;
                    while ((dir = directories.poll()) != null) {
                        final BlobListDetails blobListDetails = new BlobListDetails().setRetrieveMetadata(true);
                        final ListBlobsOptions options = (new ListBlobsOptions()).setPrefix(dir).setDetails(blobListDetails);
                        for (BlobItem blobItem : blobContainerClient.listBlobsByHierarchy("/", options, null)) {
                            boolean isPrefix = blobItem.isPrefix() != null && blobItem.isPrefix();
                            if (isPrefix == true) {
                                directories.offer(blobItem.getName());
                            } else {
                                BlobClient blobClient = blobContainerClient.getBlobClient(blobItem.getName());
                                blobURLs.add(blobClient.getBlobUrl());
                                bytesDeleted.addAndGet(blobItem.getProperties().getContentLength());
                                blobsDeleted.incrementAndGet();
                            }
                        }
                    }
                    deleteBlobListInternal(blobURLs);
                });
            }
        } catch (Exception e) {
            throw new RuntimeException("Deleting directory [" + path + "] failed", e);
        }

        return new DeleteResult(blobsDeleted.get(), bytesDeleted.get());
    }

    void deleteBlobList(List<String> blobs) {
        if (blobs.isEmpty()) {
            return;
        }

        final List<String> blobURLs = new ArrayList<>(blobs.size());
        try(final AzureBlobServiceClientRef blobServiceClient = client()) {
            final BlobServiceClient client = blobServiceClient.getClient();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobContainerClient blobContainerClient = client.getBlobContainerClient(container);
                for (String blob : blobs) {
                    blobURLs.add(blobContainerClient.getBlobClient(blob).getBlobUrl());
                }
            });
        }

        deleteBlobListInternal(blobURLs);
    }

    private void deleteBlobListInternal(List<String> blobUrls) {
        if (blobUrls.isEmpty()) {
            return;
        }

        try(final AzureBlobServiceClientRef blobServiceClient = client()) {
            final BlobServiceClient client = blobServiceClient.getClient();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(client).buildClient();
                PagedIterable<Response<Void>> responses = blobBatchClient.deleteBlobs(blobUrls, DeleteSnapshotsOptionType.ONLY);
                for (Response<Void> deleteBlob : responses) {
                    // We have to consume the results
                }
            });
        } catch (BlobBatchStorageException e) {
            for (BlobStorageException batchException : e.getBatchExceptions()) {
                if (batchException.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND) == false) {
                    throw e;
                }
            }
        }
    }


    public InputStream getInputStream(String blob, long position, @Nullable Long length) {
        logger.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", container, blob));
        final AzureBlobServiceClientRef clientRef = client();
        final BlobServiceClient client = clientRef.getClient();
        BlobServiceAsyncClient asyncClient = clientRef.getAsyncClient();

        try {
            Long realLength =
                SocketAccess.doPrivilegedException(() -> client.getBlobContainerClient(container).getBlobClient(blob).getProperties().getBlobSize());
            BlobAsyncClient blobAsyncClient =
                SocketAccess.doPrivilegedException(() -> asyncClient.getBlobContainerAsyncClient(container).getBlobAsyncClient(blob));
            return new AzureInputStream(clientRef, blobAsyncClient, position, length == null ? realLength : length, realLength,
                new BlobRequestConditions(), null);
//            final BlobInputStream is = SocketAccess.doPrivilegedException(() ->{
//                final BlobContainerClient blobContainerClient = client.getBlobContainerClient(container);
//                final BlobClient blobClient = blobContainerClient.getBlobClient(blob);
//                return blobClient.openInputStream(new BlobRange(position, length), null);
//            });
//            return new PrivilegedInputStream(clientRef, is);
        } catch (Exception e) {
            clientRef.close();
            throw e;
        }
    }

    private final static class PrivilegedInputStream extends FilterInputStream {
        private final AzureBlobServiceClientRef clientRef;
        private final BlobInputStream stream;
        private boolean closed = false;

        public PrivilegedInputStream(AzureBlobServiceClientRef clientRef, BlobInputStream stream) {
            super(stream);
            this.clientRef = clientRef;
            this.stream = stream;
        }

        @Override
        public int read() throws IOException {
            return SocketAccess.doPrivilegedIOException(stream::read);
        }

        @Override
        public int read(byte[] b) throws IOException {
            return SocketAccess.doPrivilegedIOException(() -> stream.read(b));
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return SocketAccess.doPrivilegedIOException(() -> stream.read(b, off, len));
        }

        @Override
        public void close() throws IOException {
            if (closed == false) {
                closed = true;
                clientRef.close();
                super.close();
            }
        }
    }

    public Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix) {
        // NOTE: this should be here: if (prefix == null) prefix = "";
        // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
        // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!
        final var blobsBuilder = new HashMap<String, BlobMetadata>();
        logger.trace(() ->
            new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix));
        try (AzureBlobServiceClientRef clientRef = client()) {
            final BlobServiceClient client = clientRef.getClient();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobContainerClient containerClient = client.getBlobContainerClient(container);
                final ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
                listBlobsOptions.setPrefix(keyPath + (prefix == null ? "" : prefix)).setDetails(
                    new BlobListDetails().setRetrieveMetadata(true));
                for (final BlobItem blobItem : containerClient.listBlobsByHierarchy("/", listBlobsOptions, null)) {
                    BlobItemProperties properties = blobItem.getProperties();
                    Boolean isPrefix = blobItem.isPrefix();
                    if (isPrefix != null && isPrefix) {
                        continue;
                    }
                    blobsBuilder.put(blobItem.getName(),
                        new PlainBlobMetadata(blobItem.getName(), properties.getContentLength()));
                }
            });
        }
        return Map.copyOf(blobsBuilder);
    }

    public Map<String, BlobContainer> children(BlobPath path) {
        final var blobsBuilder = new HashSet<String>();
        final String keyPath = path.buildAsString();

        try (AzureBlobServiceClientRef clientRef = client()) {
            final BlobServiceClient client = clientRef.getClient();
            SocketAccess.doPrivilegedVoidException(() -> {
                BlobContainerClient blobContainer = client.getBlobContainerClient(container);
                final ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
                listBlobsOptions.setPrefix(keyPath).setDetails(new BlobListDetails().setRetrieveMetadata(true));
                for (final BlobItem blobItem : blobContainer.listBlobsByHierarchy("/", listBlobsOptions, null)) {
                    Boolean isPrefix = blobItem.isPrefix();
                    if (isPrefix != null && isPrefix) {
                        blobsBuilder.add(blobItem.getName());
                    }
                }
            });
        }
        return Collections.unmodifiableMap(blobsBuilder.stream().collect(
            Collectors.toMap(Function.identity(), name -> new AzureBlobContainer(BlobPath.cleanPath().add(name), this))));
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        assert inputStream.markSupported()
            : "Should not be used with non-mark supporting streams as their retry handling in the SDK is broken";
        logger.info(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobName, blobSize));

        try (AzureBlobServiceClientRef blobContainerClientRef = client()) {
            final BlobServiceClient client = blobContainerClientRef.getClient();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobClient blob = client.getBlobContainerClient(container).getBlobClient(blobName);
                blob.upload(inputStream, blobSize, failIfAlreadyExists == false);
            });
        } catch (final BlobStorageException e) {
            if (failIfAlreadyExists && e.getStatusCode() == HttpURLConnection.HTTP_CONFLICT &&
                BlobErrorCode.BLOB_ALREADY_EXISTS.equals(e.getErrorCode())) {
                throw new FileAlreadyExistsException(blobName, null, e.getMessage());
            }
            throw e;
        }

        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

    private AzureBlobServiceClientRef client() {
        return service.client(clientName, locationMode, threadPool);
    }

//    private OperationContext hookMetricCollector(OperationContext context, Consumer<HttpURLConnection> metricCollector) {
//        context.getRequestCompletedEventHandler().addListener(new StorageEvent<>() {
//            @Override
//            public void eventOccurred(RequestCompletedEvent eventArg) {
//                int statusCode = eventArg.getRequestResult().getStatusCode();
//                if (statusCode < 300) {
//                    metricCollector.accept((HttpURLConnection) eventArg.getConnectionObject());
//                }
//            }
//        });
//        return context;
//    }

    @Override
    public Map<String, Long> stats() {
        return stats.toMap();
    }

    private static class Stats {

        private final AtomicLong getOperations = new AtomicLong();

        private final AtomicLong listOperations = new AtomicLong();

        private final AtomicLong headOperations = new AtomicLong();

        private final AtomicLong putOperations = new AtomicLong();

        private final AtomicLong putBlockOperations = new AtomicLong();

        private Map<String, Long> toMap() {
            return Map.of("GET", getOperations.get(),
                "LIST", listOperations.get(),
                "HEAD", headOperations.get(),
                "PUT", putOperations.get(),
                "PUT_BLOCK", putBlockOperations.get());
        }
    }
}
