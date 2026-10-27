/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The final uploaded hollow commit and its in-memory Lucene metadata, carried by primary relocation so the target can open its
 * hollow engine without object-store reads. The metadata files are a serialization of {@link SegmentInfos}, including every
 * referenced {@code .si}; the receiver reconstructs the objects for the engine factory. These are not byte-for-byte file copies
 * to persist or populate the blob cache.
 */
public record HollowShardRecoveryState(BatchedCompoundCommit commit, Map<String, byte[]> metadataFiles) implements Writeable {

    public HollowShardRecoveryState {
        if (commit.lastCompoundCommit().hollow() == false) {
            throw new IllegalArgumentException("only hollow commits can be transferred as recovery metadata");
        }
        metadataFiles = Map.copyOf(metadataFiles);
    }

    public HollowShardRecoveryState(StreamInput in) throws IOException {
        this(
            new BatchedCompoundCommit(
                new PrimaryTermAndGeneration(in),
                in.readCollectionAsList(StatelessCompoundCommit::readFromTransport)
            ),
            in.readMap(StreamInput::readByteArray)
        );
    }

    /** Captures the fenced, uploaded commit using engine memory only, without opening files on the source. */
    public static HollowShardRecoveryState capture(BatchedCompoundCommit commit, SegmentInfos infos) throws IOException {
        if (commit.lastCompoundCommit().generation() != infos.getGeneration() || IndexEngine.isLastCommitHollow(infos) == false) {
            throw new IllegalArgumentException("hollow recovery metadata must match the final uploaded commit");
        }
        return new HollowShardRecoveryState(commit, serializeSegmentInfos(infos));
    }

    /** Deserializes the engine metadata and binds its segment references to the target shard's directory. */
    public SegmentInfos readSegmentInfos(Directory targetDirectory) throws IOException {
        try (var metadataDirectory = new ByteBuffersDirectory()) {
            for (var entry : metadataFiles.entrySet()) {
                try (var output = metadataDirectory.createOutput(entry.getKey(), IOContext.DEFAULT)) {
                    output.writeBytes(entry.getValue(), entry.getValue().length);
                }
            }
            final var infos = SegmentInfos.readLatestCommit(
                metadataDirectory,
                IndexVersions.MINIMUM_READONLY_COMPATIBLE.luceneVersion().major
            );
            if (infos.getGeneration() != commit.lastCompoundCommit().generation() || IndexEngine.isLastCommitHollow(infos) == false) {
                throw new IOException("transferred segment metadata does not match the hollow commit");
            }
            // Lucene binds SegmentInfo.dir to the directory it reads from. Rebind to the real target directory before disposing of
            // the temporary serialization, so the engine never retains an in-memory directory or uses it to read segment data.
            final var segments = new ArrayList<SegmentCommitInfo>(infos.size());
            for (var segment : infos) {
                final var si = segment.info;
                final var rebound = new SegmentInfo(
                    targetDirectory,
                    si.getVersion(),
                    si.getMinVersion(),
                    si.name,
                    si.maxDoc(),
                    si.getUseCompoundFile(),
                    si.getHasBlocks(),
                    si.getCodec(),
                    si.getDiagnostics(),
                    si.getId(),
                    si.getAttributes(),
                    si.getIndexSort()
                );
                rebound.setFiles(si.files());
                final var copy = new SegmentCommitInfo(
                    rebound,
                    segment.getDelCount(),
                    segment.getSoftDelCount(),
                    segment.getDelGen(),
                    segment.getFieldInfosGen(),
                    segment.getDocValuesGen(),
                    segment.getId()
                );
                copy.setFieldInfosFiles(segment.getFieldInfosFiles());
                copy.setDocValuesUpdatesFiles(segment.getDocValuesUpdatesFiles());
                segments.add(copy);
            }
            infos.clear();
            infos.addAll(segments);
            return infos;
        }
    }

    private static Map<String, byte[]> serializeSegmentInfos(SegmentInfos infos) throws IOException {
        try (var directory = new ByteBuffersDirectory()) {
            for (var segment : infos) {
                segment.info.getCodec().segmentInfoFormat().write(directory, segment.info, IOContext.DEFAULT);
            }
            // SegmentInfos.write generates a new commit ID and records the running Lucene version. Preserve the original values:
            // this serializes an existing commit, rather than creating a new one.
            final String temporaryFile = "serialized_segments";
            try (var output = directory.createOutput(temporaryFile, IOContext.DEFAULT)) {
                infos.write(output);
            }
            try (
                var input = directory.openInput(temporaryFile, IOContext.READONCE);
                var output = directory.createOutput(infos.getSegmentsFileName(), IOContext.DEFAULT)
            ) {
                final byte[] header = CodecUtil.readIndexHeader(input);
                System.arraycopy(infos.getId(), 0, header, CodecUtil.headerLength("segments"), infos.getId().length);
                output.writeBytes(header, header.length);
                input.readVInt();
                input.readVInt();
                input.readVInt();
                final var version = infos.getCommitLuceneVersion();
                output.writeVInt(version.major);
                output.writeVInt(version.minor);
                output.writeVInt(version.bugfix);
                output.copyBytes(input, input.length() - input.getFilePointer() - CodecUtil.footerLength());
                CodecUtil.writeFooter(output);
            }
            directory.deleteFile(temporaryFile);
            final Map<String, byte[]> files = new HashMap<>();
            for (String name : directory.listAll()) {
                try (var input = directory.openInput(name, IOContext.READONCE)) {
                    final byte[] bytes = new byte[Math.toIntExact(input.length())];
                    input.readBytes(bytes, 0, bytes.length);
                    files.put(name, bytes);
                }
            }
            return files;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commit.primaryTermAndGeneration().writeTo(out);
        out.writeCollection(commit.compoundCommits());
        out.writeMap(metadataFiles, StreamOutput::writeByteArray);
    }
}
