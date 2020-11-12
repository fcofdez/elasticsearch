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

import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.UrlBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

public final class RequestRetryPolicy implements HttpPipelinePolicy {
    private final RequestRetryOptions requestRetryOptions;

    public RequestRetryPolicy(RequestRetryOptions requestRetryOptions) {
        this.requestRetryOptions = requestRetryOptions;
    }

    private final Logger logger = LogManager.getLogger(RequestRetryPolicy.class);

    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        boolean considerSecondary = this.requestRetryOptions.getSecondaryHost() != null && (HttpMethod.GET.equals(context.getHttpRequest().getHttpMethod()) || HttpMethod.HEAD.equals(context.getHttpRequest().getHttpMethod()));
        return this.attemptAsync(context, next, context.getHttpRequest(), considerSecondary, 1, 1);
    }

    private Mono<HttpResponse> attemptAsync(HttpPipelineCallContext context, HttpPipelineNextPolicy next, HttpRequest originalRequest, boolean considerSecondary, int primaryTry, int attempt) {
        boolean tryingPrimary = !considerSecondary || attempt % 2 != 0;
        long delayMs;
        if (tryingPrimary) {
            delayMs = primaryTry == 1 ? 0 : requestRetryOptions.getRetryDelayInMs();
        } else {
            delayMs = (long)(((double)(ThreadLocalRandom.current().nextFloat() / 2.0F) + 0.8D) * 1000.0D);
        }

        context.setHttpRequest(originalRequest.copy());
        Flux<ByteBuffer> bufferedBody = context.getHttpRequest().getBody() == null ? null : context.getHttpRequest().getBody().map(ByteBuffer::duplicate);
        context.getHttpRequest().setBody(bufferedBody);
        if (!tryingPrimary) {
            UrlBuilder builder = UrlBuilder.parse(context.getHttpRequest().getUrl());
            builder.setHost(this.requestRetryOptions.getSecondaryHost());

            try {
                context.getHttpRequest().setUrl(builder.toUrl());
            } catch (MalformedURLException var13) {
                return Mono.error(var13);
            }
        }

        return next.clone().process().timeout(Duration.ofSeconds((long)this.requestRetryOptions.getTryTimeout())).delaySubscription(Duration.ofMillis(delayMs)).flatMap((response) -> {
            boolean newConsiderSecondary = considerSecondary;
            int statusCode = response.getStatusCode();
            String action;
            if (!tryingPrimary && statusCode == 404) {
                newConsiderSecondary = false;
                action = "Retry: Secondary URL returned 404";
            } else if (statusCode != 503 && statusCode != 500) {
                action = "NoRetry: Successful HTTP request";
            } else {
                action = "Retry: Temporary error or server timeout";
            }

            if (action.charAt(0) == 'R' && attempt < this.requestRetryOptions.getMaxTries()) {
                int newPrimaryTry = tryingPrimary && considerSecondary ? primaryTry : primaryTry + 1;
                Flux<ByteBuffer> responseBody = response.getBody();
                if (responseBody == null) {
                    return attemptAsync(context, next, originalRequest, newConsiderSecondary, newPrimaryTry,
                        attempt + 1);
                } else {
                    return response.getBody()
                        .ignoreElements()
                        .then(attemptAsync(context, next, originalRequest, newConsiderSecondary, newPrimaryTry,
                            attempt + 1));
                }
            } else {
                return Mono.just(response);
            }
        }).onErrorResume((throwable) -> {
            if (throwable instanceof IllegalStateException && attempt > 1) {
                return Mono.error(new IllegalStateException("The request failed because the size of the contents of the provided Flux did not match the provided data size upon attempting to retry. This is likely caused by the Flux not being replayable. To support retries, all Fluxes must produce the same data for each subscriber. Please ensure this behavior.", throwable));
            } else {
                String action;
                if (throwable instanceof IOException) {
                    action = "Retry: Network error " + throwable.getMessage();
                } else if (throwable instanceof TimeoutException) {
                    action = "Retry: Client timeout";
                } else {
                    action = "NoRetry: Unknown error";
                }

                if (action.charAt(0) == 'R' && attempt < this.requestRetryOptions.getMaxTries()) {
                    int newPrimaryTry = tryingPrimary && considerSecondary ? primaryTry : primaryTry + 1;
                    return this.attemptAsync(context, next, originalRequest, considerSecondary, newPrimaryTry, attempt + 1);
                } else {
                    return Mono.error(throwable);
                }
            }
        });
    }
}
