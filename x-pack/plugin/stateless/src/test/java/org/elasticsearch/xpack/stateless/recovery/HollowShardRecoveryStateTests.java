/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Version;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Verifies that relocation metadata survives transport without access to the source's segment files. */
public class HollowShardRecoveryStateTests extends ESTestCase {

    public void testSegmentInfosRoundTrip() throws Exception {
        assertSegmentInfosRoundTrip(false);
    }

    public void testEmptySegmentInfosRoundTrip() throws Exception {
        assertSegmentInfosRoundTrip(true);
    }

    private void assertSegmentInfosRoundTrip(boolean empty) throws Exception {
        final SegmentInfos source;
        final HollowShardRecoveryState state;
        try (var directory = new ByteBuffersDirectory()) {
            try (
                var writer = new IndexWriter(
                    directory,
                    new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
                        .setUseCompoundFile(randomBoolean())
                        .setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)))
                )
            ) {
                if (empty == false) {
                    for (int i = 0; i < 12; i++) {
                        var doc = new Document();
                        doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                        doc.add(new NumericDocValuesField("sort", i));
                        doc.add(new NumericDocValuesField("value", i));
                        writer.addDocument(doc);
                        if (i % 4 == 3) {
                            writer.commit();
                        }
                    }
                    writer.deleteDocuments(new Term("id", "0"));
                    writer.updateNumericDocValue(new Term("id", "5"), "value", 42);
                }
                writer.setLiveCommitData(
                    Map.of(
                        StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE,
                        Long.toString(StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE),
                        "history_uuid",
                        "history",
                        "local_checkpoint",
                        "11",
                        "max_seq_no",
                        "11",
                        "translog_uuid",
                        "translog"
                    ).entrySet()
                );
                writer.commit();
            }
            source = SegmentInfos.readLatestCommit(directory);
            state = HollowShardRecoveryState.capture(hollowCommit(source.getGeneration()), source);
        }
        // Close the source before transferring. The receiver only has metadata, not any postings, stored fields or doc values.
        final var copy = copyWriteable(state, new NamedWriteableRegistry(List.of()), HollowShardRecoveryState::new);
        assertEquals(state.commit(), copy.commit());
        assertEquals(source.size() + 1, copy.metadataFiles().size());
        try (var target = new ByteBuffersDirectory()) {
            final var recovered = copy.readSegmentInfos(target);
            assertEquals(0, target.listAll().length);
            assertEquals(source.getGeneration(), recovered.getGeneration());
            assertEquals(source.getLastGeneration(), recovered.getLastGeneration());
            assertEquals(source.getVersion(), recovered.getVersion());
            assertEquals(source.counter, recovered.counter);
            assertEquals(source.getUserData(), recovered.getUserData());
            assertArrayEquals(source.getId(), recovered.getId());
            assertEquals(source.getCommitLuceneVersion(), recovered.getCommitLuceneVersion());
            assertEquals(source.getMinSegmentLuceneVersion(), recovered.getMinSegmentLuceneVersion());
            assertEquals(source.getIndexCreatedVersionMajor(), recovered.getIndexCreatedVersionMajor());
            assertEquals(source.files(true), recovered.files(true));
            assertEquals(source.size(), recovered.size());
            for (int i = 0; i < source.size(); i++) {
                var before = source.info(i);
                var after = recovered.info(i);
                assertSame(target, after.info.dir);
                assertEquals(before.info.name, after.info.name);
                assertArrayEquals(before.info.getId(), after.info.getId());
                assertEquals(before.info.maxDoc(), after.info.maxDoc());
                assertEquals(before.info.getVersion(), after.info.getVersion());
                assertEquals(before.info.getMinVersion(), after.info.getMinVersion());
                assertEquals(before.info.getHasBlocks(), after.info.getHasBlocks());
                assertEquals(before.info.getCodec().getName(), after.info.getCodec().getName());
                assertEquals(before.info.getIndexSort(), after.info.getIndexSort());
                assertEquals(before.info.getUseCompoundFile(), after.info.getUseCompoundFile());
                assertEquals(before.info.getAttributes(), after.info.getAttributes());
                assertEquals(before.info.getDiagnostics(), after.info.getDiagnostics());
                assertEquals(before.getDelCount(), after.getDelCount());
                assertEquals(before.getSoftDelCount(), after.getSoftDelCount());
                assertEquals(before.getDelGen(), after.getDelGen());
                assertEquals(before.getFieldInfosGen(), after.getFieldInfosGen());
                assertEquals(before.getDocValuesGen(), after.getDocValuesGen());
                assertEquals(before.getFieldInfosFiles(), after.getFieldInfosFiles());
                assertEquals(before.getDocValuesUpdatesFiles(), after.getDocValuesUpdatesFiles());
                assertArrayEquals(before.getId(), after.getId());
            }
        }
        // A stale engine snapshot must never be paired with a newer uploaded commit.
        expectThrows(
            IllegalArgumentException.class,
            () -> HollowShardRecoveryState.capture(hollowCommit(source.getGeneration() + 1), source)
        );
        // Check the receiving side too: the wire payload must not combine metadata from different commits.
        try (var target = new ByteBuffersDirectory()) {
            final var mismatched = new HollowShardRecoveryState(hollowCommit(source.getGeneration() + 1), copy.metadataFiles());
            expectThrows(IOException.class, () -> mismatched.readSegmentInfos(target));
        }
    }

    public void testHandoffTransportVersions() throws IOException {
        final var bcc = hollowCommit(1);
        final HollowShardRecoveryState state;
        try (var source = new ByteBuffersDirectory()) {
            final var infos = new SegmentInfos(Version.LATEST.major);
            infos.setUserData(
                Map.of(
                    StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE,
                    Long.toString(StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE)
                ),
                false
            );
            infos.commit(source);
            state = HollowShardRecoveryState.capture(bcc, SegmentInfos.readLatestCommit(source));
        }
        final var request = new TransportStatelessPrimaryRelocationHandoffAction.Request(
            1,
            bcc.shardId(),
            new ReplicationTracker.PrimaryContext(
                1,
                Map.of(),
                new IndexShardRoutingTable.Builder(bcc.shardId()).addShard(
                    TestShardRouting.newShardRouting(bcc.shardId(), "source", true, ShardRoutingState.STARTED)
                ).build()
            ),
            RetentionLeases.EMPTY,
            Map.of(),
            new BlobFileWithLength(bcc.toBlobFile(), bcc.calculateBccBlobLength()),
            Set.of(),
            false,
            Set.of(bcc.toBlobFile()),
            true,
            state
        );
        for (var version : List.of(
            TransportVersion.current(),
            TransportVersionUtils.randomVersionNotSupporting(TransportStatelessPrimaryRelocationHandoffAction.HOLLOW_SHARD_RECOVERY_STATE)
        )) {
            final var copy = copyWriteable(
                request,
                new NamedWriteableRegistry(List.of()),
                TransportStatelessPrimaryRelocationHandoffAction.Request::new,
                version
            );
            try (var target = new ByteBuffersDirectory()) {
                final var hints = copy.recoveryInfoFromSource(target);
                assertEquals(request.recoveryInfoFromSource(target).sourceBlobsInfo(), hints.sourceBlobsInfo());
                if (version.supports(TransportStatelessPrimaryRelocationHandoffAction.HOLLOW_SHARD_RECOVERY_STATE)) {
                    assertEquals(bcc, hints.hollowCommit());
                    assertEquals(1, hints.segmentInfos().getGeneration());
                } else {
                    assertNull(hints.hollowCommit());
                    assertNull(hints.segmentInfos());
                }
            }
        }
    }

    private static BatchedCompoundCommit hollowCommit(long generation) {
        final var termAndGeneration = new PrimaryTermAndGeneration(1, generation);
        final var commit = StatelessCompoundCommit.newHollowStatelessCompoundCommit(
            new ShardId("test", "uuid", 0),
            termAndGeneration,
            Map.of(),
            100,
            Set.of(),
            0,
            InternalFilesReplicatedRanges.EMPTY,
            Map.of(),
            null
        );
        return new BatchedCompoundCommit(termAndGeneration, List.of(commit));
    }
}
