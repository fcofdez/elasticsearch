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

import com.azure.core.util.FluxUtil;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.specialized.BlobAsyncClientBase;
import com.azure.storage.common.StorageInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;

public final class AzureInputStream extends StorageInputStream {
    private final static int CHUNK_SIZE = 4 * 1024 * 1024;
    private final AzureBlobServiceClientRef clientRef;
    private final BlobAsyncClientBase blobClient;
    private final BlobRequestConditions accessCondition;
    private final TimeValue timeout;
    private boolean closed = false;
    private final ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);

    AzureInputStream(AzureBlobServiceClientRef clientRef,
                     BlobAsyncClientBase blobClient,
                     long blobRangeOffset,
                     long blobRangeLength,
                     long blobSize,
                     BlobRequestConditions accessCondition,
                     TimeValue timeout) throws BlobStorageException {
        super(blobRangeOffset, blobRangeLength, CHUNK_SIZE, blobSize);
        this.clientRef = clientRef;
        this.blobClient = blobClient;
        this.accessCondition = accessCondition;
        this.timeout = timeout;
    }
    private final Logger logger = LogManager.getLogger(AzureInputStream.class);

    protected ByteBuffer dispatchRead(int readLength, long offset) throws IOException {
        try {
            Mono<ByteBuffer> byteBufferMono = this.blobClient.downloadWithResponse(new BlobRange(offset,
                (long) readLength), new DownloadRetryOptions(), this.accessCondition, false)
                .flatMap(response -> FluxUtil.collectBytesInByteBufferStream(response.getValue()).map(ByteBuffer::wrap));
            ByteBuffer currentBuffer = byteBufferMono.block();

            this.bufferSize = currentBuffer.remaining();
            this.bufferStartOffset = offset;
            return currentBuffer;
        } catch (BlobStorageException exception) {
            this.streamFaulted = true;
            this.lastError = new IOException(exception);
            throw this.lastError;
        }
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            super.close();
            clientRef.close();
        }
    }
}
