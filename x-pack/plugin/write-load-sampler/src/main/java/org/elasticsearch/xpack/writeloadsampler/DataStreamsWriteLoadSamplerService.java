/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadsampler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.writeloadsampler.DataStreamsWriteLoadSamplerPlugin.WRITE_LOAD_SAMPLING_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.writeloadsampler.DataStreamsWriteLoadSamplerPlugin.currentThreadIsWriterLoadSamplerThreadOrTestThread;

class DataStreamsWriteLoadSamplerService extends AbstractLifecycleComponent {
    static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "indices.write_load.sampler.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    static final Setting<TimeValue> SAMPLING_FREQUENCY_SETTING = Setting.timeSetting(
        "indices.write_load.sampler.frequency",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMillis(500),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Logger logger = LogManager.getLogger(DataStreamsWriteLoadSamplerService.class);
    private final AtomicBoolean started = new AtomicBoolean();

    private final DataStreamsWriteLoadSampler writeLoadSampler;
    private final ThreadPool threadPool;

    private volatile boolean enabled;
    private volatile TimeValue samplingFrequency;
    private volatile Scheduler.Cancellable scheduledSampling;

    DataStreamsWriteLoadSamplerService(
        DataStreamsWriteLoadSampler writeLoadSampler,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        Settings settings
    ) {
        this.writeLoadSampler = writeLoadSampler;
        this.threadPool = threadPool;
        this.enabled = ENABLED_SETTING.get(settings);
        this.samplingFrequency = SAMPLING_FREQUENCY_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(SAMPLING_FREQUENCY_SETTING, this::setSamplingFrequency);
    }

    @Override
    protected void doStart() {
        if (started.compareAndSet(false, true)) {
            maybeScheduleSampling();
        }
    }

    @Override
    protected void doStop() {
        if (started.compareAndSet(true, false)) {
            enabled = false;
            maybeCancelScheduledSampling();
        }
    }

    @Override
    protected void doClose() {

    }

    private void setSamplingFrequency(TimeValue samplingFrequency) {
        this.samplingFrequency = samplingFrequency;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            maybeCancelScheduledSampling();
        } else {
            maybeCancelScheduledSampling();
        }
    }

    private void sampleWriteLoadStats() {
        assert currentThreadIsWriterLoadSamplerThreadOrTestThread() : Thread.currentThread().getName();

        if (enabled == false) {
            return;
        }

        try {
            writeLoadSampler.sampleWriteLoadStats();
        } catch (Exception e) {
            logger.warn("Unable to collect write load stats", e);
        }

        maybeScheduleSampling();
    }

    private void maybeCancelScheduledSampling() {
        if (scheduledSampling != null) {
            scheduledSampling.cancel();
        }
    }

    private void maybeScheduleSampling() {
        if (enabled == false) {
            return;
        }

        scheduledSampling = threadPool.schedule(this::sampleWriteLoadStats, samplingFrequency, WRITE_LOAD_SAMPLING_THREAD_POOL_NAME);
    }
}
