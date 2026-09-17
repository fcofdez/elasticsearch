/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.InputStreamDataInput;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.InternalFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobLocation;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomCommitFiles;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomShardId;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomTimestampFieldValueRange;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests that the streaming header read used to build {@link BlobFileRanges} agrees with fully deserializing the
 * compound commit, for both the current and the obsolete on-disk formats.
 */
public class StatelessCompoundCommitStreamingReadTests extends ESTestCase {

    /**
     * Jackson's SMILE parser refills its input buffer in chunks of this size. A header whose length makes the last
     * refill land exactly on the closing token leaves the writer's trailing end marker unread, which is the case
     * {@link #testMatchesFullParseAtParserBufferBoundary()} pins down.
     */
    private static final int PARSER_BUFFER_SIZE = 8000;

    /** Delivers at most {@code maxChunk} bytes per bulk read, the way a blob-store backed stream does. */
    private static class ChunkedStreamInput extends FilterStreamInput {
        private final int maxChunk;

        ChunkedStreamInput(StreamInput delegate, int maxChunk) {
            super(delegate);
            this.maxChunk = maxChunk;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, Math.min(len, maxChunk));
        }
    }

    public void testMatchesFullParse() throws Exception {
        for (int iteration = 0; iteration < 20; iteration++) {
            assertStreamingMatchesFullParse(randomHeader(), randomBoolean(), 0);
        }
    }

    public void testMatchesFullParseWithChunkedStream() throws Exception {
        var header = randomHeader();
        // a chunk size of one byte makes every buffer refill end on the token the parser stops at, so the writer's
        // trailing end marker is never pulled through the checksummed stream unless the reader drains it explicitly
        for (int chunkSize : new int[] { 1, 2, 3, 7, 64, 4096 }) {
            assertStreamingMatchesFullParse(header, randomBoolean(), chunkSize);
        }
    }

    public void testMatchesFullParseAtParserBufferBoundary() throws Exception {
        // Solve for a header whose xContent region is one byte past a whole number of parser buffer fills. The header
        // length is affine in the internal file count and in the node ephemeral id length, so measure both steps. This
        // needs a fixed fixture: a randomized one changes length between measurements and the solve is meaningless.
        final int baseFileCount = 64;
        final int baseLength = xContentLengthOf(fixedHeader(baseFileCount, 1));
        final int perFile = xContentLengthOf(fixedHeader(baseFileCount + 1, 1)) - baseLength;
        final int perEphemeralIdChar = xContentLengthOf(fixedHeader(baseFileCount, 2)) - baseLength;
        assertThat(perFile, greaterThan(0));
        assertThat(perEphemeralIdChar, equalTo(1));

        int target = baseLength + 1;
        while (target % PARSER_BUFFER_SIZE != 1) {
            target++;
        }
        final int needed = target - baseLength;
        final byte[] headerBytes = fixedHeader(baseFileCount + needed / perFile, 1 + needed % perFile);

        assertThat("fixture no longer lands on a parser buffer boundary", xContentLengthOf(headerBytes) % PARSER_BUFFER_SIZE, equalTo(1));
        assertStreamingMatchesFullParse(headerBytes, randomBoolean(), 0);
    }

    public void testMatchesFullParseWithReplicatedRanges() throws Exception {
        assertStreamingMatchesFullParse(header(randomIntBetween(1, 20), 10, true, false), true, randomFrom(0, 1));
        assertStreamingMatchesFullParse(header(randomIntBetween(1, 20), 10, true, false), false, randomFrom(0, 1));
    }

    public void testMatchesFullParseWithExtraContent() throws Exception {
        assertStreamingMatchesFullParse(header(randomIntBetween(1, 20), 10, randomBoolean(), true), randomBoolean(), randomFrom(0, 1));
    }

    public void testMatchesFullParseWithObsoleteVersions() throws Exception {
        final var internalFiles = internalFiles(randomIntBetween(1, 10));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommitTests.writeBwcHeader(
                new OutputStreamStreamOutput(output),
                randomShardId(),
                randomLongBetween(1, 1000),
                randomLongBetween(1, 1000),
                randomAlphaOfLength(10),
                randomCommitFiles(),
                internalFiles,
                randomFrom(StatelessCompoundCommit.VERSION_WITH_COMMIT_FILES, StatelessCompoundCommit.VERSION_WITH_BLOB_LENGTH)
            );
            assertStreamingMatchesFullParse(BytesReference.toBytes(output.bytes()), randomBoolean(), randomFrom(0, 1));
        }
    }

    /**
     * Reads {@code headerBytes} both ways and asserts the streaming read returns the same compound commit size and the
     * same blob file ranges as fully deserializing the commit and computing the ranges from it.
     *
     * @param chunkSize maximum bytes per bulk read, or {@code 0} to leave the whole buffer available at once
     */
    private void assertStreamingMatchesFullParse(byte[] headerBytes, boolean useReplicatedRanges, int chunkSize) throws IOException {
        final long blobOffset = randomFrom(0L, 4096L, 65536L);
        final long bccGeneration = randomLongBetween(1, 1000);

        final StatelessCompoundCommit fullyParsed;
        try (StreamInput in = new ByteArrayStreamInput(headerBytes)) {
            fullyParsed = StatelessCompoundCommit.readFromStoreAtOffset(in, blobOffset, ignored -> bccGeneration);
        }

        // production only ever asks for the files the latest commit still references, and passes just their locations
        final Set<String> referencedFiles = Set.copyOf(randomNonEmptySubsetOf(fullyParsed.internalFiles()));
        final Map<String, BlobLocation> commitFilesForBlob = referencedFiles.stream()
            .collect(Collectors.toMap(Function.identity(), fullyParsed.commitFiles()::get));

        final Map<String, BlobFileRanges> expected = BlobFileRanges.computeBlobFileRanges(
            useReplicatedRanges,
            fullyParsed,
            blobOffset,
            referencedFiles
        );

        final Map<String, BlobFileRanges> actual = new HashMap<>();
        final long actualSize;
        try (StreamInput raw = new ByteArrayStreamInput(headerBytes)) {
            StreamInput in = chunkSize == 0 ? raw : new ChunkedStreamInput(raw, chunkSize);
            actualSize = StatelessCompoundCommit.readBlobFileRangesAtOffset(
                in,
                blobOffset,
                useReplicatedRanges,
                referencedFiles,
                commitFilesForBlob,
                actual
            );
        }

        assertThat(actualSize, equalTo(fullyParsed.sizeInBytes()));
        assertThat(actual.keySet(), equalTo(referencedFiles));
        assertThat(actual, equalTo(expected));
    }

    private byte[] randomHeader() throws IOException {
        return header(randomIntBetween(1, 30), randomIntBetween(1, 20), randomBoolean(), randomBoolean());
    }

    /**
     * Writes a current-format header. {@code extraContent} forces a hollow commit, which is the only kind allowed to
     * carry extra content.
     */
    private byte[] header(int internalFileCount, int nodeEphemeralIdLength, boolean withReplicatedRanges, boolean withExtraContent)
        throws IOException {
        final var internalFiles = internalFiles(internalFileCount);
        final var replicatedRanges = withReplicatedRanges ? replicatedRangesFor(internalFiles) : InternalFilesReplicatedRanges.EMPTY;
        final var extraContent = withExtraContent
            ? List.of(new InternalFile("extra_content_file", randomLongBetween(100, 1000)))
            : List.<InternalFile>of();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommit.writeXContentHeader(
                randomShardId(),
                randomLongBetween(1, 1000),
                randomLongBetween(1, 1000),
                withExtraContent ? "" : randomAlphaOfLength(nodeEphemeralIdLength),
                withExtraContent ? HOLLOW_TRANSLOG_RECOVERY_START_FILE : randomLongBetween(0, 1000),
                randomFrom(randomTimestampFieldValueRange(), null),
                randomCommitFiles(),
                internalFiles,
                replicatedRanges,
                new OutputStreamStreamOutput(output),
                withReplicatedRanges,
                extraContent
            );
            return BytesReference.toBytes(output.bytes());
        }
    }

    /**
     * A header built only from fixed-width values, so that its encoded length is a pure function of
     * {@code internalFileCount} and {@code nodeEphemeralIdLength}.
     */
    private byte[] fixedHeader(int internalFileCount, int nodeEphemeralIdLength) throws IOException {
        var commitFiles = new HashMap<String, BlobLocation>();
        for (int i = 0; i < 4; i++) {
            commitFiles.put(Strings.format("commit_file_%05d", i), createBlobLocation(100L, 100L, 1000L + i, 1000L + i));
        }
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommit.writeXContentHeader(
                new ShardId("fixed_index_name", "fixed_index_uuid_1234", 7),
                1000L,
                1000L,
                "e".repeat(nodeEphemeralIdLength),
                1000L,
                null,
                commitFiles,
                internalFiles(internalFileCount),
                InternalFilesReplicatedRanges.EMPTY,
                new OutputStreamStreamOutput(output),
                false,
                List.of()
            );
            return BytesReference.toBytes(output.bytes());
        }
    }

    /** Internal files must be strictly ordered by (length, name), and are laid out contiguously in the blob. */
    private static List<InternalFile> internalFiles(int count) {
        var files = new ArrayList<InternalFile>(count);
        for (int i = 0; i < count; i++) {
            // fixed-width name and length so that each extra file adds a constant number of bytes to the header
            files.add(new InternalFile(Strings.format("internal_file_%06d", i), 100000L + i));
        }
        return files;
    }

    /**
     * One replicated range at the start of each internal file, mirroring the replicated header the writer emits, so
     * that the range lookup in {@link BlobFileRanges#computeBlobFileRanges} actually resolves.
     */
    private static InternalFilesReplicatedRanges replicatedRangesFor(List<InternalFile> internalFiles) {
        var ranges = new ArrayList<InternalFilesReplicatedRanges.InternalFileReplicatedRange>(internalFiles.size());
        long position = 0;
        for (var internalFile : internalFiles) {
            ranges.add(
                new InternalFilesReplicatedRanges.InternalFileReplicatedRange(position, (short) Math.min(internalFile.length(), 64))
            );
            position += internalFile.length();
        }
        return InternalFilesReplicatedRanges.from(ranges);
    }

    private static int xContentLengthOf(byte[] headerBytes) throws IOException {
        try (StreamInput in = new ByteArrayStreamInput(headerBytes)) {
            CodecUtil.checkHeader(
                new InputStreamDataInput(in),
                StatelessCompoundCommit.SHARD_COMMIT_CODEC,
                StatelessCompoundCommit.VERSION_WITH_COMMIT_FILES,
                StatelessCompoundCommit.CURRENT_VERSION
            );
            return in.readInt();
        }
    }
}
