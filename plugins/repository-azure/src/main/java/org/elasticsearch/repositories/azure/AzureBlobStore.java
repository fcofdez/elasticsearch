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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
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

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;

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
        final Map<String, AzureStorageSettings> prevSettings = this.service.refreshAndClearCache(emptyMap());
        final Map<String, AzureStorageSettings> newSettings = AzureStorageSettings.overrideLocationMode(prevSettings);
        this.service.refreshAndClearCache(newSettings);
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
        return new AzureBlobContainer(path, this, threadPool);
    }

    @Override
    public void close() {
    }

    public boolean blobExists(String blob) {
        // Container name must be lower case.
        final BlobServiceClient client = client();
        //final OperationContext context = hookMetricCollector(client.v2().get(), getMetricsCollector);
        Boolean b = SocketAccess.doPrivilegedException(() -> {
            final BlobContainerClient blobContainer = client().getBlobContainerClient(container);
            final BlobClient azureBlob = blobContainer.getBlobClient(blob);
            return azureBlob.exists();
        });
        return b != null ? b : false;
    }

    public void deleteBlob(String blob) throws URISyntaxException {
        final BlobServiceClient client = client();
        //final OperationContext context = hookMetricCollector(client.v2().get(), getMetricsCollector);

        logger.trace(() -> new ParameterizedMessage("delete blob for container [{}], blob [{}]", container, blob));
        SocketAccess.doPrivilegedVoidException(() -> {
            final BlobContainerClient blobContainer = client.getBlobContainerClient(container);
            final BlobClient azureBlob = blobContainer.getBlobClient(blob);
            logger.trace(() -> new ParameterizedMessage("container [{}]: blob [{}] found. removing.", container, blob));
            azureBlob.delete();
//            azureBlob.delete(DeleteSnapshotsOption.NONE, null, null, client.v2().get());
        });
    }

    public DeleteResult deleteBlobDirectory(String path, Executor executor) {
        try {
            SocketAccess.doPrivilegedVoidException(() -> {
               BlobContainerClient blobContainerClient = client().getBlobContainerClient(container);
                for (BlobItem blobItem : blobContainerClient.listBlobsByHierarchy(path)) {
                    blobContainerClient.getBlobClient(blobItem.getName()).delete();
                }
            });
        } catch (Exception e) { }
        return new DeleteResult(0, 0);
//        final BlobServiceClient client = client();
//        //final OperationContext context = hookMetricCollector(client.v2().get(), getMetricsCollector);
//        final BlobContainerClient blobContainer = client.getBlobContainerClient(container);
//
//        ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
//        listBlobsOptions.setDetails(new BlobListDetails());
//
//        final Collection<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
//        final AtomicLong outstanding = new AtomicLong(1L);
//        final PlainActionFuture<Void> result = PlainActionFuture.newFuture();
//        final Collection blobsDeleted = new AtomicLong();
//        final AtomicLong bytesDeleted = new AtomicLong();
//        SocketAccess.doPrivilegedVoidException(() -> {
//            for (final ListBlobItem blobItem : blobContainer.listBlobs(path, true,
//                EnumSet.noneOf(BlobListingDetails.class), null, context)) {
//                // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
//                // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
//                final String blobPath = blobItem.getUri().getPath().substring(1 + container.length() + 1);
//                outstanding.incrementAndGet();
//                executor.execute(new AbstractRunnable() {
//                    @Override
//                    protected void doRun() throws Exception {
//                        final long len;
//                        if (blobItem instanceof CloudBlob) {
//                            len = ((CloudBlob) blobItem).getProperties().getLength();
//                        } else {
//                            len = -1L;
//                        }
//                        deleteBlob(blobPath);
//                        blobsDeleted.incrementAndGet();
//                        if (len >= 0) {
//                            bytesDeleted.addAndGet(len);
//                        }
//                    }
//
//                    @Override
//                    public void onFailure(Exception e) {
//                        exceptions.add(e);
//                    }
//
//                    @Override
//                    public void onAfter() {
//                        if (outstanding.decrementAndGet() == 0) {
//                            result.onResponse(null);
//                        }
//                    }
//                });
//            }
//        });
//        if (outstanding.decrementAndGet() == 0) {
//            result.onResponse(null);
//        }
//        result.actionGet();
//        if (exceptions.isEmpty() == false) {
//            final IOException ex = new IOException("Deleting directory [" + path + "] failed");
//            exceptions.forEach(ex::addSuppressed);
//            throw ex;
//        }
//        return new DeleteResult(blobsDeleted.get(), bytesDeleted.get());
    }

    public InputStream getInputStream(String blob, long position, @Nullable Long length) {
        final BlobContainerClient blobContainer = getBlobContainer();
        BlobClient blobClient = SocketAccess.doPrivilegedException(() -> {
            assert blobContainer != null;
            return blobContainer.getBlobClient(blob);
        });

        logger.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", container, blob));
        assert blobClient != null;
        final BlobInputStream is = SocketAccess.doPrivilegedException(() ->
            blobClient.openInputStream(new BlobRange(position, length), null));
        return giveSocketPermissionsToStream(is);
    }

    private BlobContainerClient getBlobContainer() {
        return SocketAccess.doPrivilegedException(() -> {
            BlobContainerClient blobContainerClient = client().getBlobContainerClient(container);
            return blobContainerClient;
        });
    }

    public Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix)
        throws URISyntaxException {
        // NOTE: this should be here: if (prefix == null) prefix = "";
        // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
        // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!
        final var blobsBuilder = new HashMap<String, BlobMetadata>();
        final BlobContainerClient blobContainer = getBlobContainer();
        logger.trace(() ->
            new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix));
        SocketAccess.doPrivilegedVoidException(() -> {
            final ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(keyPath + (prefix == null ? "" : prefix)).setDetails(
                new BlobListDetails().setRetrieveMetadata(true));
            for (final BlobItem blobItem : blobContainer.listBlobs(listBlobsOptions, null)) {
                blobsBuilder.put(blobItem.getName(),
                    new PlainBlobMetadata(blobItem.getName(), blobItem.getProperties().getContentLength()));
            }
        });
        return Map.copyOf(blobsBuilder);
    }

    public Map<String, BlobContainer> children(BlobPath path) {
//        final var blobsBuilder = new HashSet<String>();
//        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
//        final OperationContext context = hookMetricCollector(client.v2().get(), listMetricsCollector);
//        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
//        final String keyPath = path.buildAsString();
//        final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);
//
//        SocketAccess.doPrivilegedVoidException(() -> {
//            for (ListBlobItem blobItem : blobContainer.listBlobs(keyPath, false, enumBlobListingDetails, null, context)) {
//                if (blobItem instanceof CloudBlobDirectory) {
//                    final URI uri = blobItem.getUri();
//                    logger.trace(() -> new ParameterizedMessage("blob url [{}]", uri));
//                    // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
//                    // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /.
//                    // Lastly, we add the length of keyPath to the offset to strip this container's path.
//                    final String uriPath = uri.getPath();
//                    blobsBuilder.add(uriPath.substring(1 + container.length() + 1 + keyPath.length(), uriPath.length() - 1));
//                }
//            }
//        });
//
//        return Collections.unmodifiableMap(blobsBuilder.stream().collect(
//            Collectors.toMap(Function.identity(), name -> new AzureBlobContainer(path.add(name), this, threadPool))));
        return Collections.emptyMap();
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws URISyntaxException {
        assert inputStream.markSupported()
            : "Should not be used with non-mark supporting streams as their retry handling in the SDK is broken";
        logger.info(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobName, blobSize));
//        try {

//            final AccessCondition accessCondition =
//                failIfAlreadyExists ? AccessCondition.generateIfNotExistsCondition() : AccessCondition.generateEmptyCondition();
        SocketAccess.doPrivilegedVoidException(() -> {
            BlobContainerClient blobContainerClient = client().getBlobContainerClient(container);
            final BlobClient blob = blobContainerClient.getBlobClient(blobName);
            blob.upload(inputStream, blobSize, failIfAlreadyExists == false);
        });
//                blob.uploadWithResponse(inputStream, blobSize, ParallelTransferOptions)
//                blob.upload(inputStream, blobSize, accessCondition, service.getBlobRequestOptionsForWriteBlob(), operationContext));
//        } catch (final StorageException se) {
//            if (failIfAlreadyExists && se.getHttpStatusCode() == HttpURLConnection.HTTP_CONFLICT &&
//                StorageErrorCodeStrings.BLOB_ALREADY_EXISTS.equals(se.getErrorCode())) {
//                throw new FileAlreadyExistsException(blobName, null, se.getMessage());
//            }
//            throw se;
//        }
        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

    private BlobServiceClient client() {
        return service.client(clientName);
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

    static InputStream giveSocketPermissionsToStream(final InputStream stream) {
        return new InputStream() {
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
        };
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
