/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.kv;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class RocksEngine extends Engine {
    public static final Setting<Boolean> INDEX_KV = Setting.boolSetting("index.kv", false, Setting.Property.IndexScope);

    private final Logger logger = LogManager.getLogger(RocksEngine.class);
    private final RocksDB db;
    private final AtomicLong currentSeqNo = new AtomicLong();

    public RocksEngine(EngineConfig engineConfig) {
        super(engineConfig);
        RocksDB.loadLibrary();
        try {
            this.db = RocksDB.open(engineConfig.getShardId().toString());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SegmentInfos getLastCommittedSegmentInfos() {
        return new SegmentInfos(6);
    }

    @Override
    public CommitStats commitStats() {
        return new CommitStats(Map.of(), 1, "", 1);
    }

    @Override
    public String getHistoryUUID() {
        return "hola";
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return null;
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {

    }

    @Override
    public IndexResult index(Index index) throws IOException {
        try {
            db.put(index.id().getBytes(), index.source().array());
            logger.info("--> added {}", Arrays.toString(db.get(index.id().getBytes())));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
        var seqNo = currentSeqNo.incrementAndGet();
        IndexResult indexResult = new IndexResult(1, 1, seqNo, true, index.id());
        indexResult.setTranslogLocation(new Translog.Location(0, 0, 0));
        return indexResult;
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        try {
            db.delete(delete.id().getBytes());
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
        return new DeleteResult(1, 1, 1, true, "id");
    }

    @Override
    public NoOpResult noOp(NoOp noOp) throws IOException {
        return new NoOpResult(1, 1);
    }

    @Override
    public GetResult get(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(byte[] id) {
        try {
            return db.get(id);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public RocksDB getDb() {
        return db;
    }

    @Override
    protected ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        return new ReferenceManager<>() {
            @Override
            protected void decRef(ElasticsearchDirectoryReader reference) throws IOException {

            }

            @Override
            protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
                return null;
            }

            @Override
            protected boolean tryIncRef(ElasticsearchDirectoryReader reference) throws IOException {
                return false;
            }

            @Override
            protected int getRefCount(ElasticsearchDirectoryReader reference) {
                return 1;
            }
        };
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public void asyncEnsureTranslogSynced(Translog.Location location, Consumer<Exception> listener) {
        listener.accept(null);
    }

    @Override
    public void asyncEnsureGlobalCheckpointSynced(long globalCheckpoint, Consumer<Exception> listener) {

    }

    @Override
    public void syncTranslog() throws IOException {

    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return () -> {};
    }

    @Override
    public int countChanges(String source, long fromSeqNo, long toSeqNo) throws IOException {
        return 0;
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats
    ) throws IOException {
        return Translog.Snapshot.EMPTY;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return true;
    }

    @Override
    public long getMinRetainedSeqNo() {
        return 0;
    }

    @Override
    public TranslogStats getTranslogStats() {
        return null;
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return Translog.Location.EMPTY;
    }

    @Override
    public long getMaxSeqNo() {
        return 0;
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return 0;
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return 0;
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return new SeqNoStats(1, 1, 1);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return 0;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments() {
        return List.of();
    }

    @Override
    public List<Segment> segments(boolean includeVectorFormatsInfo) {
        return List.of();
    }

    @Override
    public RefreshResult refresh(String source) throws EngineException {
        return new RefreshResult(true);
    }

    @Override
    public void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException {

    }

    @Override
    public void writeIndexingBuffer() throws IOException {
        try {
            db.flushWal(false);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    protected void flushHoldingLock(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) throws EngineException {
        try {
            db.flush(new FlushOptions().setWaitForFlush(true));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void trimUnreferencedTranslogFiles() throws EngineException {

    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {

    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID) throws EngineException,
        IOException {
        try {
            db.compactRange();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return null;
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return null;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return new SafeCommitInfo(1, 1);
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        db.close();
    }

    @Override
    public void activateThrottling() {

    }

    @Override
    public void deactivateThrottling() {

    }

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        return 0;
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public void recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo, ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    @Override
    public void skipTranslogRecovery() {

    }

    @Override
    public void maybePruneDeletes() {

    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {

    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return 0;
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {

    }

    @Override
    public ShardLongFieldRange getRawFieldRange(String field) throws IOException {
        return null;
    }
}
