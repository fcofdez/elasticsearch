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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Keeps track of state related to shard recovery.
 */
public class RecoveryState implements ToXContentFragment, Writeable {

    public enum Stage {
        INIT((byte) 0),

        /**
         * recovery of lucene files, either reusing local ones are copying new ones
         */
        INDEX((byte) 1),

        /**
         * potentially running check index
         */
        VERIFY_INDEX((byte) 2),

        /**
         * starting up the engine, replaying the translog
         */
        TRANSLOG((byte) 3),

        /**
         * performing final task after all translog ops have been done
         */
        FINALIZE((byte) 4),

        DONE((byte) 5),

        /**
         * recovery will be performed lazily as searches hit the shard
         */
        ON_DEMAND((byte) 6);

        private static final Stage[] STAGES = new Stage[Stage.values().length];

        static {
            for (Stage stage : Stage.values()) {
                assert stage.id() < STAGES.length && stage.id() >= 0;
                STAGES[stage.id] = stage;
            }
        }

        private final byte id;

        Stage(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Stage fromId(byte id) {
            if (id < 0 || id >= STAGES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STAGES[id];
        }
    }

    private Stage stage;

    private final Index index;
    private final Translog translog;
    private final VerifyIndex verifyIndex;
    private final Timer timer;

    private RecoverySource recoverySource;
    private ShardId shardId;
    @Nullable
    private DiscoveryNode sourceNode;
    private DiscoveryNode targetNode;
    private boolean primary;

    public RecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode) {
        this(shardRouting, targetNode, sourceNode, new Index());
    }

    protected RecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode, Index index) {
        assert shardRouting.initializing() : "only allow initializing shard routing to be recovered: " + shardRouting;
        RecoverySource recoverySource = shardRouting.recoverySource();
        assert (recoverySource.getType() == RecoverySource.Type.PEER) == (sourceNode != null) :
            "peer recovery requires source node, recovery type: " + recoverySource.getType() + " source node: " + sourceNode;
        this.shardId = shardRouting.shardId();
        this.primary = shardRouting.primary();
        this.recoverySource = recoverySource;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        stage = Stage.INIT;
        this.index = index;
        translog = new Translog();
        verifyIndex = new VerifyIndex();
        timer = new Timer();
        timer.start();
    }

    public RecoveryState(StreamInput in) throws IOException {
        timer = new Timer(in);
        stage = Stage.fromId(in.readByte());
        shardId = new ShardId(in);
        recoverySource = RecoverySource.readFrom(in);
        targetNode = new DiscoveryNode(in);
        sourceNode = in.readOptionalWriteable(DiscoveryNode::new);
        index = new Index(in);
        translog = new Translog(in);
        verifyIndex = new VerifyIndex(in);
        primary = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        timer.writeTo(out);
        out.writeByte(stage.id());
        shardId.writeTo(out);
        recoverySource.writeTo(out);
        targetNode.writeTo(out);
        out.writeOptionalWriteable(sourceNode);
        index.writeTo(out);
        translog.writeTo(out);
        verifyIndex.writeTo(out);
        out.writeBoolean(primary);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public synchronized Stage getStage() {
        return this.stage;
    }

    private void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            assert false : "can't move recovery to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])";
            throw new IllegalStateException("can't move recovery to stage [" + next + "]. current stage: ["
                    + stage + "] (expected [" + expected + "])");
        }
        stage = next;
    }

    public synchronized void validateCurrentStage(Stage expected) {
        if (stage != expected) {
            assert false : "expected stage [" + expected + "]; but current stage is [" + stage + "]";
            throw new IllegalStateException("expected stage [" + expected + "] but current stage is [" + stage + "]");
        }
    }

    // synchronized is strictly speaking not needed (this is called by a single thread), but just to be safe
    public synchronized RecoveryState setStage(Stage stage) {
        switch (stage) {
            case INIT:
                // reinitializing stop remove all state except for start time
                this.stage = Stage.INIT;
                getIndex().reset();
                getVerifyIndex().reset();
                getTranslog().reset();
                break;
            case INDEX:
                validateAndSetStage(Stage.INIT, stage);
                getIndex().start();
                break;
            case VERIFY_INDEX:
                validateAndSetStage(Stage.INDEX, stage);
                getIndex().stop();
                getVerifyIndex().start();
                break;
            case TRANSLOG:
                validateAndSetStage(Stage.VERIFY_INDEX, stage);
                getVerifyIndex().stop();
                getTranslog().start();
                break;
            case FINALIZE:
                assert getIndex().bytesStillToRecover() >= 0 : "moving to stage FINALIZE without completing file details";
                validateAndSetStage(Stage.TRANSLOG, stage);
                getTranslog().stop();
                break;
            case DONE:
                validateAndSetStage(Stage.FINALIZE, stage);
                getTimer().stop();
                break;
            case ON_DEMAND:
                validateAndSetStage(Stage.FINALIZE, stage);
                break;
            default:
                throw new IllegalArgumentException("unknown RecoveryState.Stage [" + stage + "]");
        }
        return this;
    }

    public Index getIndex() {
        return index;
    }

    public VerifyIndex getVerifyIndex() {
        return this.verifyIndex;
    }

    public Translog getTranslog() {
        return translog;
    }

    public Timer getTimer() {
        return timer;
    }

    public RecoverySource getRecoverySource() {
        return recoverySource;
    }

    /**
     * Returns recovery source node (only non-null if peer recovery)
     */
    @Nullable
    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public boolean getPrimary() {
        return primary;
    }

    public static RecoveryState readRecoveryState(StreamInput in) throws IOException {
        return new RecoveryState(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field(Fields.ID, shardId.id());
        builder.field(Fields.TYPE, recoverySource.getType());
        builder.field(Fields.STAGE, stage.toString());
        builder.field(Fields.PRIMARY, primary);
        builder.timeField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, timer.startTime);
        if (timer.stopTime > 0) {
            builder.timeField(Fields.STOP_TIME_IN_MILLIS, Fields.STOP_TIME, timer.stopTime);
        }
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(timer.time()));

        if (recoverySource.getType() == RecoverySource.Type.PEER) {
            builder.startObject(Fields.SOURCE);
            builder.field(Fields.ID, sourceNode.getId());
            builder.field(Fields.HOST, sourceNode.getHostName());
            builder.field(Fields.TRANSPORT_ADDRESS, sourceNode.getAddress().toString());
            builder.field(Fields.IP, sourceNode.getHostAddress());
            builder.field(Fields.NAME, sourceNode.getName());
            builder.endObject();
        } else {
            builder.startObject(Fields.SOURCE);
            recoverySource.addAdditionalFields(builder, params);
            builder.endObject();
        }

        builder.startObject(Fields.TARGET);
        builder.field(Fields.ID, targetNode.getId());
        builder.field(Fields.HOST, targetNode.getHostName());
        builder.field(Fields.TRANSPORT_ADDRESS, targetNode.getAddress().toString());
        builder.field(Fields.IP, targetNode.getHostAddress());
        builder.field(Fields.NAME, targetNode.getName());
        builder.endObject();

        builder.startObject(Fields.INDEX);
        index.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.TRANSLOG);
        translog.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.VERIFY_INDEX);
        verifyIndex.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    static final class Fields {
        static final String ID = "id";
        static final String TYPE = "type";
        static final String STAGE = "stage";
        static final String PRIMARY = "primary";
        static final String START_TIME = "start_time";
        static final String START_TIME_IN_MILLIS = "start_time_in_millis";
        static final String STOP_TIME = "stop_time";
        static final String STOP_TIME_IN_MILLIS = "stop_time_in_millis";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String SOURCE = "source";
        static final String HOST = "host";
        static final String TRANSPORT_ADDRESS = "transport_address";
        static final String IP = "ip";
        static final String NAME = "name";
        static final String TARGET = "target";
        static final String INDEX = "index";
        static final String TRANSLOG = "translog";
        static final String TOTAL_ON_START = "total_on_start";
        static final String VERIFY_INDEX = "verify_index";
        static final String RECOVERED = "recovered";
        static final String RECOVERED_IN_BYTES = "recovered_in_bytes";
        static final String CHECK_INDEX_TIME = "check_index_time";
        static final String CHECK_INDEX_TIME_IN_MILLIS = "check_index_time_in_millis";
        static final String LENGTH = "length";
        static final String LENGTH_IN_BYTES = "length_in_bytes";
        static final String FILES = "files";
        static final String TOTAL = "total";
        static final String TOTAL_IN_BYTES = "total_in_bytes";
        static final String REUSED = "reused";
        static final String REUSED_IN_BYTES = "reused_in_bytes";
        static final String PERCENT = "percent";
        static final String DETAILS = "details";
        static final String SIZE = "size";
        static final String SOURCE_THROTTLE_TIME = "source_throttle_time";
        static final String SOURCE_THROTTLE_TIME_IN_MILLIS = "source_throttle_time_in_millis";
        static final String TARGET_THROTTLE_TIME = "target_throttle_time";
        static final String TARGET_THROTTLE_TIME_IN_MILLIS = "target_throttle_time_in_millis";
    }

    public static class Timer implements Writeable {
        protected long startTime = 0;
        protected long startNanoTime = 0;
        protected long time = -1;
        protected long stopTime = 0;

        public Timer() {
        }

        public Timer(StreamInput in) throws IOException {
            startTime = in.readVLong();
            startNanoTime = in.readVLong();
            stopTime = in.readVLong();
            time = in.readVLong();
        }

        @Override
        public synchronized void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(startTime);
            out.writeVLong(startNanoTime);
            out.writeVLong(stopTime);
            // write a snapshot of current time, which is not per se the time field
            out.writeVLong(time());
        }

        public synchronized void start() {
            assert startTime == 0 : "already started";
            startTime = System.currentTimeMillis();
            startNanoTime = System.nanoTime();
        }

        /** Returns start time in millis */
        public synchronized long startTime() {
            return startTime;
        }

        /** Returns elapsed time in millis, or 0 if timer was not started */
        public synchronized long time() {
            if (startNanoTime == 0) {
                return 0;
            }
            if (time >= 0) {
                return time;
            }
            return Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - startNanoTime));
        }

        /** Returns stop time in millis */
        public synchronized long stopTime() {
            return stopTime;
        }

        public synchronized void stop() {
            assert stopTime == 0 : "already stopped";
            stopTime = Math.max(System.currentTimeMillis(), startTime);
            time = TimeValue.nsecToMSec(System.nanoTime() - startNanoTime);
            assert time >= 0;
        }

        public synchronized void reset() {
            startTime = 0;
            startNanoTime = 0;
            time = -1;
            stopTime = 0;
        }

        // for tests
        public long getStartNanoTime() {
            return startNanoTime;
        }
    }

    public static class VerifyIndex extends Timer implements ToXContentFragment, Writeable {
        private volatile long checkIndexTime;

        public VerifyIndex() {
        }

        public VerifyIndex(StreamInput in) throws IOException {
            super(in);
            checkIndexTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(checkIndexTime);
        }

        public void reset() {
            super.reset();
            checkIndexTime = 0;
        }

        public long checkIndexTime() {
            return checkIndexTime;
        }

        public void checkIndexTime(long checkIndexTime) {
            this.checkIndexTime = checkIndexTime;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(Fields.CHECK_INDEX_TIME_IN_MILLIS, Fields.CHECK_INDEX_TIME, new TimeValue(checkIndexTime));
            builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            return builder;
        }
    }

    public static class Translog extends Timer implements ToXContentFragment, Writeable {
        public static final int UNKNOWN = -1;

        private int recovered;
        private int total = UNKNOWN;
        private int totalOnStart = UNKNOWN;
        private int totalLocal = UNKNOWN;

        public Translog() {
        }

        public Translog(StreamInput in) throws IOException {
            super(in);
            recovered = in.readVInt();
            total = in.readVInt();
            totalOnStart = in.readVInt();
            if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
                totalLocal = in.readVInt();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(recovered);
            out.writeVInt(total);
            out.writeVInt(totalOnStart);
            if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
                out.writeVInt(totalLocal);
            }
        }

        public synchronized void reset() {
            super.reset();
            recovered = 0;
            total = UNKNOWN;
            totalOnStart = UNKNOWN;
            totalLocal = UNKNOWN;
        }

        public synchronized void incrementRecoveredOperations() {
            recovered++;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" + total +
                "], recovered [" + recovered + "]";
        }

        public synchronized void incrementRecoveredOperations(int ops) {
            recovered += ops;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" + total +
                "], recovered [" + recovered + "]";
        }

        public synchronized void decrementRecoveredOperations(int ops) {
            recovered -= ops;
            assert recovered >= 0 : "recovered operations must be non-negative. Because [" + recovered +
                "] after decrementing [" + ops + "]";
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" +
                total + "], recovered [" + recovered + "]";
        }


        /**
         * returns the total number of translog operations recovered so far
         */
        public synchronized int recoveredOperations() {
            return recovered;
        }

        /**
         * returns the total number of translog operations needed to be recovered at this moment.
         * Note that this can change as the number of operations grows during recovery.
         * <p>
         * A value of -1 ({@link RecoveryState.Translog#UNKNOWN} is return if this is unknown (typically a gateway recovery)
         */
        public synchronized int totalOperations() {
            return total;
        }

        public synchronized void totalOperations(int total) {
            this.total = totalLocal == UNKNOWN ? total : totalLocal + total;
            assert total == UNKNOWN || this.total >= recovered : "total, if known, should be > recovered. total [" + total +
                "], recovered [" + recovered + "]";
        }

        /**
         * returns the total number of translog operations to recovered, on the start of the recovery. Unlike {@link #totalOperations}
         * this does change during recovery.
         * <p>
         * A value of -1 ({@link RecoveryState.Translog#UNKNOWN} is return if this is unknown (typically a gateway recovery)
         */
        public synchronized int totalOperationsOnStart() {
            return this.totalOnStart;
        }

        public synchronized void totalOperationsOnStart(int total) {
            this.totalOnStart = totalLocal == UNKNOWN ? total : totalLocal + total;
        }

        /**
         * Sets the total number of translog operations to be recovered locally before performing peer recovery
         * @see IndexShard#recoverLocallyUpToGlobalCheckpoint()
         */
        public synchronized void totalLocal(int totalLocal) {
            assert totalLocal >= recovered : totalLocal + " < " + recovered;
            this.totalLocal = totalLocal;
        }

        public synchronized int totalLocal() {
            return totalLocal;
        }

        public synchronized float recoveredPercent() {
            if (total == UNKNOWN) {
                return -1.f;
            }
            if (total == 0) {
                return 100.f;
            }
            return recovered * 100.0f / total;
        }

        @Override
        public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.RECOVERED, recovered);
            builder.field(Fields.TOTAL, total);
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredPercent()));
            builder.field(Fields.TOTAL_ON_START, totalOnStart);
            builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            return builder;
        }
    }

    public static class FileDetail implements ToXContentObject, Writeable {
        private final String name;
        private final long length;
        private final boolean reused;
        private final AtomicLong recovered = new AtomicLong();

        public FileDetail(String name, long length, boolean reused) {
            assert name != null;
            this.name = name;
            this.length = length;
            this.reused = reused;
        }

        public FileDetail(StreamInput in) throws IOException {
            name = in.readString();
            length = in.readVLong();
            recovered.set(in.readVLong());
            reused = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(length);
            out.writeVLong(recovered.get());
            out.writeBoolean(reused);
        }

        public void addRecoveredBytes(long bytes) {
            assert reused == false : "file is marked as reused, can't update recovered bytes";
            assert bytes >= 0 : "can't recovered negative bytes. got [" + bytes + "]";
            recovered.getAndAccumulate(bytes, (addedBytes, recoveredBytes) -> {
                // Enforce that at most length bytes are recovered as in some
                // scenarios it is possible that an overlapping range is reported
                // multiple times
                recoveredBytes = Math.min(recoveredBytes, length - addedBytes);
                return addedBytes + recoveredBytes;
            });
        }

        public void resetRecovered() {
            assert reused == false : "file is marked as reused, can't reset the recovered bytes of a reused file";
            recovered.set(0);
        }

        /**
         * file name *
         */
        public String name() {
            return name;
        }

        /**
         * file length *
         */
        public long length() {
            return length;
        }

        /**
         * number of bytes recovered for this file (so far). 0 if the file is reused *
         */
        public long recovered() {
            return recovered.get();
        }

        /**
         * returns true if the file is reused from a local copy
         */
        public boolean reused() {
            return reused;
        }

        boolean fullyRecovered() {
            return reused == false && recovered() == 0;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, name);
            builder.humanReadableField(Fields.LENGTH_IN_BYTES, Fields.LENGTH, new ByteSizeValue(length));
            builder.field(Fields.REUSED, reused);
            builder.humanReadableField(Fields.RECOVERED_IN_BYTES, Fields.RECOVERED, new ByteSizeValue(recovered()));
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FileDetail) {
                FileDetail other = (FileDetail) obj;
                return name.equals(other.name) && length == other.length() && reused == other.reused() && recovered() == other.recovered();
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + Long.hashCode(length);
            result = 31 * result + Long.hashCode(recovered());
            result = 31 * result + (reused ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "file (name [" + name + "], reused [" + reused + "], length [" + length + "], recovered [" + recovered() + "])";
        }
    }

    public static class RecoveryFilesDetails implements ToXContentFragment, Writeable {
        protected final Map<String, FileDetail> fileDetails = ConcurrentCollections.newConcurrentMap();
        protected final AtomicBoolean complete = new AtomicBoolean();

        public RecoveryFilesDetails() {
        }

        public RecoveryFilesDetails(StreamInput in) throws IOException {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                FileDetail file = new FileDetail(in);
                fileDetails.put(file.name, file);
            }
            if (in.getVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
                complete.set(in.readBoolean());
            } else {
                // This flag is used by disk-based allocation to decide whether the remaining bytes measurement is accurate or not; if not
                // then it falls back on an estimate. There's only a very short window in which the file details are present but incomplete
                // so this is a reasonable approximation, and the stats reported to the disk-based allocator don't hit this code path
                // anyway since they always use IndexShard#getRecoveryState which is never transported over the wire.
                complete.set(fileDetails.isEmpty() == false);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final FileDetail[] files = values().toArray(new FileDetail[0]);
            out.writeVInt(files.length);
            for (FileDetail file : files) {
                file.writeTo(out);
            }
            if (out.getVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
                out.writeBoolean(complete.get());
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (params.paramAsBoolean("detailed", false)) {
                builder.startArray(Fields.DETAILS);
                for (FileDetail file : values()) {
                    file.toXContent(builder, params);
                }
                builder.endArray();
            }

            return builder;
        }

        public void addFileDetails(String name, long length, boolean reused) {
            assert isComplete() == false : "addFileDetail for [" + name + "] when file details are already complete";
            FileDetail existing = fileDetails.put(name, new FileDetail(name, length, reused));
            assert existing == null : "file [" + name + "] is already reported";
        }

        public void addRecoveredBytesToFile(String name, long bytes) {
            FileDetail file = fileDetails.computeIfPresent(name, (n, fileDetail) -> {
                fileDetail.addRecoveredBytes(bytes);
                return fileDetail;
            });
            assert file != null : "file [" + name + "] hasn't been reported";
        }

        public void resetRecoveredBytesOfFile(String name) {
            FileDetail file = fileDetails.computeIfPresent(name, (n, fileDetail) -> {
                fileDetail.resetRecovered();
                return fileDetail;
            });
            assert file != null : "file [" + name + "] hasn't been reported";
        }

        public FileDetail get(String name) {
            return fileDetails.get(name);
        }

        public void setComplete() {
            complete.set(true);
        }

        public int size() {
            return fileDetails.size();
        }

        public boolean isEmpty() {
            return fileDetails.isEmpty();
        }

        public void clear() {
            fileDetails.clear();
            complete.set(false);
        }

        public Collection<FileDetail> values() {
            return fileDetails.values();
        }

        public boolean isComplete() {
            return complete.get();
        }
    }

    public static class Index extends Timer implements ToXContentFragment, Writeable {
        public static final long UNKNOWN = -1L;

        protected final RecoveryFilesDetails fileDetails;
        private final AtomicLong sourceThrottlingInNanos;
        private final AtomicLong targetThrottleTimeInNanos;

        public Index() {
            this(new RecoveryFilesDetails(), UNKNOWN, UNKNOWN);
        }

        public Index(StreamInput in) throws IOException {
            super(in);
            this.fileDetails = new RecoveryFilesDetails(in);
            this.sourceThrottlingInNanos = new AtomicLong(in.readLong());
            this.targetThrottleTimeInNanos = new AtomicLong(in.readLong());
        }

        public Index(RecoveryFilesDetails fileDetails, long sourceThrottlingInNanos, long targetThrottleTimeInNanos) {
            this.fileDetails = fileDetails;
            this.sourceThrottlingInNanos = new AtomicLong(sourceThrottlingInNanos);
            this.targetThrottleTimeInNanos = new AtomicLong(targetThrottleTimeInNanos);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            fileDetails.writeTo(out);
            out.writeLong(sourceThrottlingInNanos.get());
            out.writeLong(targetThrottleTimeInNanos.get());
        }

        public List<FileDetail> fileDetails() {
            return List.copyOf(fileDetails.values());
        }

        public synchronized void reset() {
            super.reset();
            fileDetails.clear();
            sourceThrottlingInNanos.set(UNKNOWN);
            targetThrottleTimeInNanos.set(UNKNOWN);
        }

        public void addFileDetail(String name, long length, boolean reused) {
            fileDetails.addFileDetails(name, length, reused);
        }

        public void setFileDetailsComplete() {
            fileDetails.setComplete();
        }

        public void addRecoveredBytesToFile(String name, long bytes) {
            fileDetails.addRecoveredBytesToFile(name, bytes);
        }

        public void addSourceThrottling(long timeInNanos) {
            sourceThrottlingInNanos.getAndAccumulate(timeInNanos, (sourceThrottlingInNanos, time) -> {
                if (sourceThrottlingInNanos == UNKNOWN) {
                    return time;
                } else {
                    return sourceThrottlingInNanos + time;
                }
            });
        }

        public void addTargetThrottling(long timeInNanos) {
            targetThrottleTimeInNanos.getAndAccumulate(timeInNanos, (targetThrottleTimeInNanos, time) -> {
                if (targetThrottleTimeInNanos == UNKNOWN) {
                    return time;
                } else {
                    return targetThrottleTimeInNanos + time;
                }
            });
        }

        public TimeValue sourceThrottling() {
            return TimeValue.timeValueNanos(sourceThrottlingInNanos.get());
        }

        public TimeValue targetThrottling() {
            return TimeValue.timeValueNanos(targetThrottleTimeInNanos.get());
        }

        /**
         * total number of files that are part of this recovery, both re-used and recovered
         */
        public int totalFileCount() {
            return fileDetails.size();
        }

        /**
         * total number of files to be recovered (potentially not yet done)
         */
        public int totalRecoverFiles() {
            int total = 0;
            for (FileDetail file : fileDetails.values()) {
                if (file.reused() == false) {
                    total++;
                }
            }
            return total;
        }

        /**
         * number of file that were recovered (excluding on ongoing files)
         */
        public int recoveredFileCount() {
            int count = 0;
            for (FileDetail file : fileDetails.values()) {
                if (file.fullyRecovered()) {
                    count++;
                }
            }
            return count;
        }

        /**
         * percent of recovered (i.e., not reused) files out of the total files to be recovered
         */
        public float recoveredFilesPercent() {
            int total = 0;
            int recovered = 0;
            for (FileDetail file : fileDetails.values()) {
                if (file.reused() == false) {
                    total++;
                    if (file.fullyRecovered()) {
                        recovered++;
                    }
                }
            }
            if (total == 0 && fileDetails.size() == 0) {      // indicates we are still in init phase
                return 0.0f;
            }
            if (total == recovered) {
                return 100.0f;
            } else {
                float result = 100.0f * (recovered / (float) total);
                return result;
            }
        }

        /**
         * total number of bytes in th shard
         */
        public long totalBytes() {
            long total = 0;
            for (FileDetail file : fileDetails.values()) {
                total += file.length();
            }
            return total;
        }

        /**
         * total number of bytes recovered so far, including both existing and reused
         */
        public long recoveredBytes() {
            long recovered = 0;
            for (FileDetail file : fileDetails.values()) {
                recovered += file.recovered();
            }
            return recovered;
        }

        /**
         * total bytes of files to be recovered (potentially not yet done)
         */
        public long totalRecoverBytes() {
            long total = 0;
            for (FileDetail file : fileDetails.values()) {
                if (file.reused() == false) {
                    total += file.length();
                }
            }
            return total;
        }

        /**
         * @return number of bytes still to recover, i.e. {@link Index#totalRecoverBytes()} minus {@link Index#recoveredBytes()}, or
         * {@code -1} if the full set of files to recover is not yet known
         */
        public long bytesStillToRecover() {
            if (fileDetails.isComplete() == false) {
                return -1L;
            }
            long total = 0L;
            for (FileDetail file : fileDetails.values()) {
                if (file.reused() == false) {
                    total += file.length() - file.recovered();
                }
            }
            return total;
        }

        /**
         * percent of bytes recovered out of total files bytes *to be* recovered
         */
        public float recoveredBytesPercent() {
            long total = 0;
            long recovered = 0;
            for (FileDetail file : fileDetails.values()) {
                if (file.reused() == false) {
                    total += file.length();
                    recovered += file.recovered();
                }
            }
            if (total == 0 && fileDetails.size() == 0) {
                // indicates we are still in init phase
                return 0.0f;
            }
            if (total == recovered) {
                return 100.0f;
            } else {
                return 100.0f * recovered / total;
            }
        }

        public int reusedFileCount() {
            int reused = 0;
            for (FileDetail file : fileDetails.values()) {
                if (file.reused()) {
                    reused++;
                }
            }
            return reused;
        }

        public long reusedBytes() {
            long reused = 0;
            for (FileDetail file : fileDetails.values()) {
                if (file.reused()) {
                    reused += file.length();
                }
            }
            return reused;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // stream size first, as it matters more and the files section can be long
            builder.startObject(Fields.SIZE);
            builder.humanReadableField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, new ByteSizeValue(totalBytes()));
            builder.humanReadableField(Fields.REUSED_IN_BYTES, Fields.REUSED, new ByteSizeValue(reusedBytes()));
            builder.humanReadableField(Fields.RECOVERED_IN_BYTES, Fields.RECOVERED, new ByteSizeValue(recoveredBytes()));
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredBytesPercent()));
            builder.endObject();

            builder.startObject(Fields.FILES);
            builder.field(Fields.TOTAL, totalFileCount());
            builder.field(Fields.REUSED, reusedFileCount());
            builder.field(Fields.RECOVERED, recoveredFileCount());
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredFilesPercent()));
            fileDetails.toXContent(builder, params);
            builder.endObject();
            builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            builder.humanReadableField(Fields.SOURCE_THROTTLE_TIME_IN_MILLIS, Fields.SOURCE_THROTTLE_TIME, sourceThrottling());
            builder.humanReadableField(Fields.TARGET_THROTTLE_TIME_IN_MILLIS, Fields.TARGET_THROTTLE_TIME, targetThrottling());
            return builder;
        }

        @Override
        public String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
                builder.startObject();
                toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                return Strings.toString(builder);
            } catch (IOException e) {
                return "{ \"error\" : \"" + e.getMessage() + "\"}";
            }
        }

        public FileDetail getFileDetails(String dest) {
            return fileDetails.get(dest);
        }
    }
}
