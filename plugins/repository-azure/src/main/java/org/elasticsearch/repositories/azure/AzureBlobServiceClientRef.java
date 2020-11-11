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

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import io.netty.channel.EventLoopGroup;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import reactor.netty.resources.ConnectionProvider;

class AzureBlobServiceClientRef extends AbstractRefCounted implements Releasable {
    private final BlobServiceClient blobServiceClient;
    private final BlobServiceAsyncClient blobAsyncClient;
    private final EventLoopGroup eventLoopGroup;
    private final TimeValue timeout;
    private final ConnectionProvider connectionProvider;

    public AzureBlobServiceClientRef(String name,
                                     BlobServiceClient blobServiceClient,
                                     BlobServiceAsyncClient blobAsyncClient,
                                     EventLoopGroup eventLoopGroup,
                                     TimeValue timeout, ConnectionProvider provider) {
        super(name);
        this.blobServiceClient = blobServiceClient;
        this.blobAsyncClient = blobAsyncClient;
        this.eventLoopGroup = eventLoopGroup;
        this.timeout = timeout;
        this.connectionProvider = provider;
    }

    public BlobServiceClient getClient() {
        return blobServiceClient;
    }

    public BlobServiceAsyncClient getAsyncClient() {
        return blobAsyncClient;
    }

    @Override
    protected void closeInternal() {
        connectionProvider.dispose();
        eventLoopGroup.shutdownGracefully();
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public void close() {
        decRef();
    }
}
