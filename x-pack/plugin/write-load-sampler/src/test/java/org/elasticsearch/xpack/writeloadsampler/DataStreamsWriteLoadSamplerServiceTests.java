/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadsampler;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.writeloadsampler.DataStreamsWriteLoadSamplerPlugin.WRITE_LOAD_SAMPLING_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class DataStreamsWriteLoadSamplerServiceTests extends ESTestCase {
    public void testSamplingIsScheduled() throws Exception {
        final var samplingFrequency = TimeValue.timeValueSeconds(2);
        final var settings = Settings.builder()
            .put(DataStreamsWriteLoadSamplerService.SAMPLING_FREQUENCY_SETTING.getKey(), samplingFrequency)
            .build();
        runTest(settings, testContext -> {
            final var threadPool = testContext.threadPool();
            final var statsCollector = testContext.writeLoadSampler();
            final var scheduledTasks = threadPool.getScheduledTasks();
            assertThat(scheduledTasks, hasSize(1));

            {
                final var scheduledTask = threadPool.runNextTask();
                assertThat(scheduledTask.delay, is(equalTo(samplingFrequency)));
                // The task is rescheduled after executing
                assertThat(scheduledTasks, hasSize(1));
            }

            assertThat(statsCollector.samplingCalls.get(), is(equalTo(1)));
        });
    }

    public void testSamplingIsRescheduledOnlyAfterRunning() throws Exception {
        runTest(testContext -> {
            final var threadPool = testContext.threadPool();
            final var scheduledTasks = threadPool.getScheduledTasks();
            assertThat(scheduledTasks, hasSize(1));

            final var samplingTask = scheduledTasks.remove();

            assertThat(scheduledTasks, is(empty()));

            samplingTask.run();
            assertThat(scheduledTasks, hasSize(1));
        });
    }

    public void testTasksAreCancelledAfterDisablingTheService() throws Exception {
        final var settings = Settings.builder().put(DataStreamsWriteLoadSamplerService.ENABLED_SETTING.getKey(), true).build();
        runTest(settings, testContext -> {
            final var threadPool = testContext.threadPool();
            final var scheduledTasks = threadPool.getScheduledTasks();

            final var clusterSettings = testContext.clusterSettings();
            final var updatedSettings = Settings.builder().put(DataStreamsWriteLoadSamplerService.ENABLED_SETTING.getKey(), false).build();
            clusterSettings.applySettings(updatedSettings);

            assertThat(scheduledTasks, hasSize(1));
            var samplingTask = scheduledTasks.remove();
            assertThat(samplingTask.isCancelled(), is(equalTo(true)));
            assertThat(scheduledTasks, is(empty()));
        });
    }

    public void testStatsAreNotCollectedWhenServiceIsDisabled() throws Exception {
        final var settings = Settings.builder().put(DataStreamsWriteLoadSamplerService.ENABLED_SETTING.getKey(), false).build();
        runTest(settings, testContext -> {
            final var threadPool = testContext.threadPool();
            final var scheduledTasks = threadPool.getScheduledTasks();

            assertThat(scheduledTasks, is(empty()));
        });
    }

    private void runTest(Consumer<TestContext> testBody) throws Exception {
        runTest(Settings.EMPTY, testBody);
    }

    private void runTest(Settings settings, Consumer<TestContext> testBody) throws Exception {
        SequentialSchedulerThreadPool testThreadPool = new SequentialSchedulerThreadPool();

        final var clusterSettings = new ClusterSettings(
            settings,
            Set.of(DataStreamsWriteLoadSamplerService.ENABLED_SETTING, DataStreamsWriteLoadSamplerService.SAMPLING_FREQUENCY_SETTING)
        );

        final var statsCollector = new InstrumentedWriteLoadSampler();

        try (
            var indicesWriteLoadStatsService = new DataStreamsWriteLoadSamplerService(
                statsCollector,
                testThreadPool,
                clusterSettings,
                settings
            )
        ) {
            indicesWriteLoadStatsService.start();
            testBody.accept(new TestContext(indicesWriteLoadStatsService, statsCollector, testThreadPool, clusterSettings));
        } finally {
            testThreadPool.shutdown();
            testThreadPool.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    static class InstrumentedWriteLoadSampler extends DataStreamsWriteLoadSampler {
        final AtomicInteger samplingCalls = new AtomicInteger();

        InstrumentedWriteLoadSampler() {
            super(() -> ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder().build()).build(), () -> 0L);
        }

        @Override
        void sampleWriteLoadStats() {
            samplingCalls.incrementAndGet();
            if (randomBoolean()) {
                throw new RuntimeException("Failed sampling write load");
            }
        }
    }

    static class SequentialSchedulerThreadPool extends TestThreadPool {
        final Queue<ScheduledTask> scheduledTasks = new ArrayDeque<>();

        SequentialSchedulerThreadPool() {
            super(getTestClass().getName());
        }

        @Override
        public ScheduledCancellable schedule(Runnable command, TimeValue delay, String executor) {
            assertThat(executor, is(equalTo(WRITE_LOAD_SAMPLING_THREAD_POOL_NAME)));
            var scheduledTask = new ScheduledTask(command, delay);
            scheduledTasks.add(scheduledTask);
            return scheduledTask;
        }

        ScheduledTask runNextTask() {
            var scheduledTask = scheduledTasks.remove();
            if (scheduledTask != null) {
                scheduledTask.run();
            }
            return scheduledTask;
        }

        Queue<ScheduledTask> getScheduledTasks() {
            return scheduledTasks;
        }
    }

    static class ScheduledTask implements Scheduler.ScheduledCancellable, Runnable {
        private final Runnable command;
        private final TimeValue delay;
        private final AtomicBoolean cancelled = new AtomicBoolean();

        ScheduledTask(Runnable command, TimeValue delay) {
            this.command = command;
            this.delay = delay;
        }

        @Override
        public void run() {
            assert cancelled.get() == false;
            command.run();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel() {
            return cancelled.compareAndSet(false, true);
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }
    }

    record TestContext(
        DataStreamsWriteLoadSamplerService samplerService,
        InstrumentedWriteLoadSampler writeLoadSampler,
        SequentialSchedulerThreadPool threadPool,
        ClusterSettings clusterSettings
    ) {}
}
