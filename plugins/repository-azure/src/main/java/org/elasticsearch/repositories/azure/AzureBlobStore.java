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

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
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
    // See https://docs.microsoft.com/en-us/rest/api/storageservices/blob-batch#request-body
    private static final int MAX_ELEMENTS_PER_BATCH = 256;

    private final AzureStorageService service;

    private final String clientName;
    private final String container;
    private final LocationMode locationMode;
    private final ByteSizeValue chunkSize;

    private final Stats stats = new Stats();

    private final Consumer<HttpURLConnection> getMetricsCollector;
    private final Consumer<HttpURLConnection> listMetricsCollector;
    private final Consumer<HttpURLConnection> uploadMetricsCollector;

    public AzureBlobStore(RepositoryMetadata metadata, AzureStorageService service) {
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.clientName = Repository.CLIENT_NAME.get(metadata.settings());
        this.service = service;
        // locationMode is set per repository, not per client
        this.locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        this.chunkSize = Repository.CHUNK_SIZE_SETTING.get(metadata.settings());
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
        final BlobServiceClient client = client();

        Boolean blobExists = SocketAccess.doPrivilegedException(() -> {
            final BlobClient azureBlob = client.getBlobContainerClient(container).getBlobClient(blob);
            return azureBlob.exists();
        });
        stats.headOperations.incrementAndGet();
        return blobExists != null ? blobExists : false;
    }

    public DeleteResult deleteBlobDirectory(String path) {
        final AtomicInteger blobsDeleted = new AtomicInteger(0);
        final AtomicLong bytesDeleted = new AtomicLong(0);

        try {
            final BlobServiceClient client = client();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobContainerClient blobContainerClient = client.getBlobContainerClient(container);
                final List<String> blobURLs = new ArrayList<>();
                final Queue<String> directories = new ArrayDeque<>();
                directories.offer(path);
                String directoryName;
                while ((directoryName = directories.poll()) != null) {
                    final BlobListDetails blobListDetails = new BlobListDetails()
                        .setRetrieveMetadata(true);

                    final ListBlobsOptions options = new ListBlobsOptions()
                        .setPrefix(directoryName)
                        .setDetails(blobListDetails);

                    for (BlobItem blobItem : blobContainerClient.listBlobsByHierarchy("/", options, null)) {
                        if (blobItem.isPrefix() != null && blobItem.isPrefix()) {
                            directories.offer(blobItem.getName());
                        } else {
                            BlobClient blobClient = blobContainerClient.getBlobClient(blobItem.getName());
                            blobURLs.add(blobClient.getBlobUrl());
                            bytesDeleted.addAndGet(blobItem.getProperties().getContentLength());
                            blobsDeleted.incrementAndGet();
                        }
                    }
                }
                deleteBlobsInBatches(blobURLs);
            });

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
        try {
            final BlobServiceClient client = client();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobContainerClient blobContainerClient = client.getBlobContainerClient(container);
                for (String blob : blobs) {
                    blobURLs.add(blobContainerClient.getBlobClient(blob).getBlobUrl());
                }
            });
        } catch (RuntimeException e) {
            throw new RuntimeException("Unable to delete blobs " + blobs, e);
        }

        deleteBlobsInBatches(blobURLs);
    }

    private void deleteBlobsInBatches(List<String> blobUrls) {
        if (blobUrls.isEmpty()) {
            return;
        }

        try {
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobBatchAsyncClient blobBatchClient =
                    new BlobBatchClientBuilder(asyncClient())
                        .buildAsyncClient();

                int numBatches = (int) Math.ceil((double) blobUrls.size() / (double) MAX_ELEMENTS_PER_BATCH);
                List<BlobBatch> batches = new ArrayList<>(numBatches);
                for (int batchNumber = 0; batchNumber < numBatches; batchNumber++) {
                    final BlobBatch blobBatch = blobBatchClient.getBlobBatch();
                    int rangeStart = batchNumber * MAX_ELEMENTS_PER_BATCH;
                    for (int i = rangeStart; i < Math.min(rangeStart + MAX_ELEMENTS_PER_BATCH, blobUrls.size()); i++) {
                        blobBatch.deleteBlob(blobUrls.get(i));
                    }
                    batches.add(blobBatch);
                }

                List<Mono<Response<Void>>> batchResponses = new ArrayList<>(batches.size());
                for (BlobBatch batch : batches) {
                    batchResponses.add(blobBatchClient.submitBatchWithResponse(batch, false));
                }

                Flux.merge(batchResponses).collectList().block();
            });
        } catch (BlobBatchStorageException e) {
            for (BlobStorageException batchException : e.getBatchExceptions()) {
                if (batchException.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND) == false) {
                    throw e;
                }
            }
        }
    }

    public InputStream getInputStream(String blob, long position, @Nullable Long length) throws IOException {
        logger.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", container, blob));
        final BlobServiceClient client = client();

        return SocketAccess.doPrivilegedException(() ->{
            final BlobContainerClient blobContainerClient = client.getBlobContainerClient(container);
            final BlobClient blobClient = blobContainerClient.getBlobClient(blob);
            return blobClient.openInputStream(new BlobRange(position, length), null);
        });
    }

    public Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix) throws IOException {
        final var blobsBuilder = new HashMap<String, BlobMetadata>();
        logger.trace(() ->
            new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix));
        try {
            final BlobServiceClient client = client();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobContainerClient containerClient = client.getBlobContainerClient(container);
                final BlobListDetails details = new BlobListDetails().setRetrieveMetadata(true);
                final ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
                    .setPrefix(keyPath + (prefix == null ? "" : prefix))
                    .setDetails(details);

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
        } catch (RuntimeException e) {
            throw new IOException("Unable to list blobs by prefix [" + prefix + "] for path " + keyPath, e);
        }
        return Map.copyOf(blobsBuilder);
    }

    public Map<String, BlobContainer> children(BlobPath path) throws IOException {
        final var blobsBuilder = new HashSet<String>();
        final String keyPath = path.buildAsString();

        try {
            final BlobServiceClient client = client();
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
        } catch (RuntimeException e) {
            throw new IOException("Unable to provide children blob containers for " + path, e);
        }

        return Collections.unmodifiableMap(blobsBuilder.stream().collect(
            Collectors.toMap(Function.identity(), name -> new AzureBlobContainer(BlobPath.cleanPath().add(name), this))));
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        assert inputStream.markSupported()
            : "Should not be used with non-mark supporting streams as their retry handling in the SDK is broken";
        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobName, blobSize));
        try {
            final BlobServiceClient client = client();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobClient blob = client.getBlobContainerClient(container)
                    .getBlobClient(blobName);

                ParallelTransferOptions parallelTransferOptions = getParallelTransferOptions();
                BlobParallelUploadOptions blobParallelUploadOptions =
                    new BlobParallelUploadOptions(inputStream, blobSize)
                        .setParallelTransferOptions(parallelTransferOptions);
                blob.uploadWithResponse(blobParallelUploadOptions, null, null);
            });
        } catch (final BlobStorageException e) {
            if (failIfAlreadyExists && e.getStatusCode() == HttpURLConnection.HTTP_CONFLICT &&
                BlobErrorCode.BLOB_ALREADY_EXISTS.equals(e.getErrorCode())) {
                throw new FileAlreadyExistsException(blobName, null, e.getMessage());
            }
            throw new IOException("Unable to write blob " + blobName, e);
        } catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException("Unable to write blob " + blobName, e);
        }

        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

    private ParallelTransferOptions getParallelTransferOptions() {
        ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions();
        parallelTransferOptions.setBlockSizeLong(service.getUploadBlockSize())
            .setMaxSingleUploadSizeLong(service.getSizeThresholdForMultiBlockUpload())
            .setMaxConcurrency(service.getMaxUploadParallelism());
        return parallelTransferOptions;
    }

    private BlobServiceClient client() {
        return service.client(clientName, locationMode).getSyncClient();
    }

    private BlobServiceAsyncClient asyncClient() {
        return service.client(clientName, locationMode).getAsyncClient();
    }

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
