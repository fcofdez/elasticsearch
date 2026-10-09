/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetadataMappingService.PreflightCacheEntry;
import org.elasticsearch.cluster.metadata.MetadataMappingService.PutMappingClusterStateUpdateTask;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;

public class MetadataMappingServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testMappingClusterStateUpdateDoesntChangeExistingIndices() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test").setMapping());
        final CompressedXContent currentMapping = indexService.mapperService().documentMapper().mappingSource();

        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        // TODO - it will be nice to get a random mapping generator
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        // the task really was a mapping update
        assertThat(
            indexService.mapperService().documentMapper().mappingSource(),
            not(equalTo(resultingState.metadata().getProject().index("test").mapping().source()))
        );
        // since we never committed the cluster state update, the in-memory state is unchanged
        assertThat(indexService.mapperService().documentMapper().mappingSource(), equalTo(currentMapping));
    }

    public void testMappingUpdateInProjectUnderDeletion() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final ProjectId projectId = randomUniqueProjectId();
        final Metadata metadata = Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexService.getMetadata(), false)).build();
        final ClusterState initialState = ClusterState.builder(getInstanceFromNode(ClusterService.class).state())
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .blocks(ClusterBlocks.builder().addProjectGlobalBlock(projectId, ProjectMetadata.PROJECT_UNDER_DELETION_BLOCK))
            .build();
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);

        final ClusterState resultingState = ClusterStateTaskExecutorUtils.executeHandlingResults(
            initialState,
            mappingService.new PutMappingExecutor(),
            singleTask(request),
            task -> fail("mapping update should have failed"),
            (task, e) -> {
                assertThat(e, instanceOf(ClusterBlockException.class));
                final ClusterBlockException clusterBlockException = (ClusterBlockException) e;
                assertTrue(clusterBlockException.blocks().contains(ProjectMetadata.PROJECT_UNDER_DELETION_BLOCK));
            }
        );
        assertSame(initialState, resultingState);
    }

    public void testClusterStateIsNotChangedWithIdenticalMappings() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));

        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );
        final var resultingState1 = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        final var resultingState2 = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            resultingState1,
            putMappingExecutor,
            singleTask(request)
        );
        assertSame(resultingState1, resultingState2);
    }

    public void testMappingVersion() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final long previousVersion = indexService.getMetadata().getMappingVersion();
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        assertThat(resultingState.metadata().getProject().index("test").getMappingVersion(), equalTo(1 + previousVersion));
        assertThat(resultingState.metadata().getProject().index("test").getMappingsUpdatedVersion(), equalTo(IndexVersion.current()));
    }

    public void testMappingVersionUnchanged() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test").setMapping());
        final long previousVersion = indexService.getMetadata().getMappingVersion();
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "{ \"properties\": {}}",
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        assertThat(resultingState.metadata().getProject().index("test").getMappingVersion(), equalTo(previousVersion));
    }

    public void testUpdateSettings() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final long previousVersion = indexService.getMetadata().getSettingsVersion();
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor(
            new IndexSettingProviders(Set.of(new IndexSettingProvider() {
                @Override
                public void provideAdditionalSettings(
                    String indexName,
                    String dataStreamName,
                    IndexMode templateIndexMode,
                    boolean registryInstalledTemplate,
                    ProjectMetadata projectMetadata,
                    Instant resolvedAt,
                    Settings indexTemplateAndCreateRequestSettings,
                    List<CompressedXContent> combinedTemplateMappings,
                    IndexVersion indexVersion,
                    Settings.Builder additionalSettings
                ) {}

                @Override
                public void onUpdateMappings(
                    IndexMetadata indexMetadata,
                    DocumentMapper documentMapper,
                    Settings.Builder additionalSettings
                ) {
                    additionalSettings.put("index.mapping.total_fields.limit", 42);
                }
            }))
        );
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "text" }}}""",
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );

        IndexMetadata indexMetadata = resultingState.metadata().indexMetadata(indexService.index());
        assertThat(indexMetadata.getSettingsVersion(), equalTo(1 + previousVersion));
        assertThat(indexMetadata.getSettings().get("index.mapping.total_fields.limit"), equalTo("42"));
    }

    /**
     * Test that putting an identical mapping results in a no-op and does not submit a cluster state update task.
     */
    public void testMappingNoOpUpdateExactEquals() throws IOException {
        runNoOpMappingUpdateTest("""
            {"_doc":{"properties":{"field":{"type":"keyword"}}}}""");
    }

    /**
     * Test that putting a mapping that is semantically identical but syntactically different results in a no-op.
     */
    public void testMappingNoOpUpdateSemanticEquals() throws IOException {
        runNoOpMappingUpdateTest("""
            {"properties": {"field": {"type": "keyword", "ignore_above": "2147483647"}}}""");
    }

    @SuppressWarnings("unchecked")
    private void runNoOpMappingUpdateTest(String updatedMapping) throws IOException {
        // Create index with initial mapping
        final var indexService = createIndex("test", Settings.EMPTY, "field", "type=keyword");

        // Set up MetadataMappingService with a mocked ClusterService that prevents and monitors task submission
        final ClusterService clusterService = Mockito.spy(getInstanceFromNode(ClusterService.class));
        final MasterServiceTaskQueue<PutMappingClusterStateUpdateTask> masterServiceTaskQueue = Mockito.mock(MasterServiceTaskQueue.class);
        Mockito.doThrow(new AssertionError("not supposed to run")).when(masterServiceTaskQueue).submitTask(any(), any(), any());
        Mockito.when(clusterService.<PutMappingClusterStateUpdateTask>createTaskQueue(any(), any(), any()))
            .thenReturn(masterServiceTaskQueue);
        final MetadataMappingService metadataMappingService = new MetadataMappingService(
            clusterService,
            getInstanceFromNode(IndicesService.class),
            IndexSettingProviders.EMPTY
        );

        // Put updated mapping
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            updatedMapping,
            randomBoolean(),
            indexService.index()
        );
        metadataMappingService.putMapping(request, future);
        safeGet(future);

        // Verify that no cluster state update task was submitted
        Mockito.verifyNoInteractions(masterServiceTaskQueue);
    }

    /** Cache hit: pre-flight service is reused and mapping version increments. */
    public void testCacheHitAppliesMapping() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final Index index = indexService.index();
        final IndexMetadata indexMetadata = clusterService.state().metadata().indexMetadata(index);
        final String newMapping = """
            { "properties": { "field": { "type": "keyword" }}}""";

        final Map<Index, PreflightCacheEntry> preflightCache = new HashMap<>();
        preflightCache.put(index, buildCacheEntry(indicesService, indexMetadata, newMapping));
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            newMapping,
            false,
            index
        );

        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            List.of(new PutMappingClusterStateUpdateTask(request, ActionListener.wrap(r -> {}, e -> {}), preflightCache))
        );

        assertThat(resultingState.metadata().indexMetadata(index).getMappingVersion(), equalTo(indexMetadata.getMappingVersion() + 1));
        assertThat(resultingState.metadata().indexMetadata(index).mapping().source().string(), containsString("\"field\""));
        // The success-path finally block in execute() closes and clears the cache map.
        assertTrue(preflightCache.isEmpty());
    }

    /** Stale mapping version: cache entry is discarded and a fresh service is used. */
    public void testCacheMissStaleMappingVersionFallsBack() throws Exception {
        runCacheMissTest(-1, 0);
    }

    /** Stale settings version: cache entry is discarded (settings are baked in at creation time). */
    public void testCacheMissStaleSettingsVersionFallsBack() throws Exception {
        runCacheMissTest(0, -1);
    }

    /** onFailure on a never-executed task closes all cache entries and clears the map. */
    public void testOnFailureClosesAndClearsPreflightCache() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final Index index = indexService.index();
        final IndexMetadata indexMetadata = clusterService.state().metadata().indexMetadata(index);
        final String mapping = """
            { "properties": { "field": { "type": "keyword" }}}""";

        final Map<Index, PreflightCacheEntry> preflightCache = new HashMap<>();
        preflightCache.put(index, buildCacheEntry(indicesService, indexMetadata, mapping));
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            mapping,
            false,
            index
        );
        final PutMappingClusterStateUpdateTask task = new PutMappingClusterStateUpdateTask(
            request,
            ActionListener.wrap(r -> fail("should not succeed"), e -> {}),
            preflightCache
        );

        task.onFailure(new RuntimeException("simulated failure"));

        assertTrue("preflightCache should be cleared after onFailure", preflightCache.isEmpty());
    }

    /** Batch: second task's unconsumed cache entry for a shared index is closed on success. */
    public void testBatchingSecondTaskCacheEntryIsClearedOnSuccess() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final Index index = indexService.index();
        final IndexMetadata indexMetadata = clusterService.state().metadata().indexMetadata(index);
        final String mapping1 = """
            { "properties": { "field1": { "type": "keyword" }}}""";
        final String mapping2 = """
            { "properties": { "field2": { "type": "keyword" }}}""";

        // Task 2 carries a pre-flight cache entry for the same index as task 1. After task 1
        // succeeds, the index is already in indexMapperServices, so task 2's cache entry is
        // never consumed and must be closed by the success-path finally block.
        final Map<Index, PreflightCacheEntry> preflightCache2 = new HashMap<>();
        preflightCache2.put(index, buildCacheEntry(indicesService, indexMetadata, mapping2));

        final PutMappingClusterStateUpdateRequest request1 = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            mapping1,
            false,
            index
        );
        final PutMappingClusterStateUpdateRequest request2 = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            mapping2,
            false,
            index
        );

        ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            List.of(
                new PutMappingClusterStateUpdateTask(request1, ActionListener.wrap(r -> {}, e -> {}), new HashMap<>()),
                new PutMappingClusterStateUpdateTask(request2, ActionListener.wrap(r -> {}, e -> {}), preflightCache2)
            )
        );

        // Task 2's pre-flight cache entry was unconsumed; the success-path finally block must have
        // closed and cleared it.
        assertTrue("task2 preflightCache should be cleared on success", preflightCache2.isEmpty());
    }

    /** Shared body: builds a stale cache entry (versions offset by deltas) and asserts fall-back. */
    private void runCacheMissTest(long mappingVersionDelta, long settingsVersionDelta) throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final Index index = indexService.index();
        final IndexMetadata indexMetadata = clusterService.state().metadata().indexMetadata(index);
        // The stale cache entry uses a different field name than the request so we can confirm the
        // stale entry was discarded and the request mapping was applied from a fresh service.
        final String staleMapping = """
            { "properties": { "stale_field": { "type": "keyword" }}}""";
        final String requestMapping = """
            { "properties": { "field": { "type": "keyword" }}}""";

        final Map<Index, PreflightCacheEntry> preflightCache = new HashMap<>();
        preflightCache.put(
            index,
            buildCacheEntry(
                indicesService,
                indexMetadata,
                staleMapping,
                indexMetadata.getMappingVersion() + mappingVersionDelta,
                indexMetadata.getSettingsVersion() + settingsVersionDelta
            )
        );
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            requestMapping,
            false,
            index
        );

        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            List.of(new PutMappingClusterStateUpdateTask(request, ActionListener.wrap(r -> {}, e -> {}), preflightCache))
        );

        assertThat(resultingState.metadata().indexMetadata(index).getMappingVersion(), equalTo(indexMetadata.getMappingVersion() + 1));
        final String appliedMapping = resultingState.metadata().indexMetadata(index).mapping().source().string();
        assertThat(appliedMapping, containsString("\"field\""));
        assertThat(appliedMapping, not(containsString("\"stale_field\"")));
        assertTrue(preflightCache.isEmpty());
    }

    /** Mirrors what {@code isWholeRequestNoop} does on the MANAGEMENT thread to build a cache entry. */
    private PreflightCacheEntry buildCacheEntry(IndicesService indicesService, IndexMetadata indexMetadata, String mapping)
        throws IOException {
        return buildCacheEntry(
            indicesService,
            indexMetadata,
            mapping,
            indexMetadata.getMappingVersion(),
            indexMetadata.getSettingsVersion()
        );
    }

    private PreflightCacheEntry buildCacheEntry(
        IndicesService indicesService,
        IndexMetadata indexMetadata,
        String mapping,
        long mappingVersion,
        long settingsVersion
    ) throws IOException {
        final MapperService service = indicesService.createIndexMapperServiceForValidation(indexMetadata);
        service.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
        final CompressedXContent preUpdateSource = service.documentMapper() != null ? service.documentMapper().mappingSource() : null;
        service.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        return new PreflightCacheEntry(mappingVersion, settingsVersion, preUpdateSource, service);
    }

    private record ExecutionOutcome(ClusterState state, List<String> taskResults) {}

    private ExecutionOutcome execute(ClusterState state, boolean batchingEnabled, List<PutMappingClusterStateUpdateRequest> requests)
        throws Exception {
        return execute(state, batchingEnabled, IndexSettingProviders.EMPTY, requests);
    }

    private ExecutionOutcome execute(
        ClusterState state,
        boolean batchingEnabled,
        IndexSettingProviders providers,
        List<PutMappingClusterStateUpdateRequest> requests
    ) throws Exception {
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final List<PutMappingClusterStateUpdateTask> tasks = requests.stream()
            .map(r -> new PutMappingClusterStateUpdateTask(r, ActionListener.noop(), new HashMap<>()))
            .toList();
        final String[] results = new String[tasks.size()];
        final ClusterState resultingState = ClusterStateTaskExecutorUtils.executeHandlingResults(
            state,
            mappingService.new PutMappingExecutor(providers, batchingEnabled),
            tasks,
            task -> results[identityIndexOf(tasks, task)] = "success",
            (task, e) -> results[identityIndexOf(tasks, task)] = e.getClass().getName() + ": " + e.getMessage()
        );
        return new ExecutionOutcome(resultingState, Arrays.asList(results));
    }

    private static int identityIndexOf(List<PutMappingClusterStateUpdateTask> tasks, PutMappingClusterStateUpdateTask task) {
        for (int i = 0; i < tasks.size(); i++) {
            if (tasks.get(i) == task) {
                return i;
            }
        }
        throw new AssertionError("unknown task");
    }

    private static PutMappingClusterStateUpdateRequest request(boolean autoUpdate, String properties, Index... indices) throws IOException {
        return new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "{\"properties\":{" + properties + "}}",
            autoUpdate,
            indices
        );
    }

    private static String field(String name, String type) {
        return "\"" + name + "\":{\"type\":\"" + type + "\"}";
    }

    private Index createIndexWithExistingField(String name) {
        return createIndex(name, client().admin().indices().prepareCreate(name).setMapping("existing", "type=keyword")).index();
    }

    private static CompressedXContent mappingSource(ClusterState state, Index index) {
        return state.metadata().getProject().index(index).mapping().source();
    }

    public void testBatchedAutoUpdatesForOneIndexBumpMappingVersionOnce() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final long previousVersion = initial.metadata().getProject().index(index).getMappingVersion();
        final List<PutMappingClusterStateUpdateRequest> requests = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            requests.add(request(true, field("field" + i, randomFrom("long", "keyword")), index));
        }

        final ExecutionOutcome batched = execute(initial, true, requests);
        final ExecutionOutcome sequential = execute(initial, false, requests);

        assertThat(batched.taskResults(), equalTo(Collections.nCopies(requests.size(), "success")));
        assertThat(batched.taskResults(), equalTo(sequential.taskResults()));
        assertThat(mappingSource(batched.state(), index), equalTo(mappingSource(sequential.state(), index)));
        assertThat(batched.state().metadata().getProject().index(index).getMappingVersion(), equalTo(previousVersion + 1));
        assertThat(sequential.state().metadata().getProject().index(index).getMappingVersion(), equalTo(previousVersion + requests.size()));
    }

    public void testBatchedAutoUpdatesForInterleavedIndices() throws Exception {
        final List<Index> indices = List.of(
            createIndexWithExistingField("test1"),
            createIndexWithExistingField("test2"),
            createIndexWithExistingField("test3")
        );
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final List<PutMappingClusterStateUpdateRequest> requests = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            requests.add(request(true, field("field" + i, "long"), indices.get(i % indices.size())));
        }

        final ExecutionOutcome batched = execute(initial, true, requests);
        final ExecutionOutcome sequential = execute(initial, false, requests);

        assertThat(batched.taskResults(), equalTo(Collections.nCopies(requests.size(), "success")));
        for (Index index : indices) {
            assertThat(mappingSource(batched.state(), index), equalTo(mappingSource(sequential.state(), index)));
            assertThat(
                batched.state().metadata().getProject().index(index).getMappingVersion(),
                equalTo(initial.metadata().getProject().index(index).getMappingVersion() + 1)
            );
        }
    }

    public void testFailingTaskInGroupFallsBackToPerTaskBehavior() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final List<PutMappingClusterStateUpdateRequest> requests = List.of(
            request(true, field("field1", "long"), index),
            request(true, field("field2", "long"), index),
            request(true, "\"existing\":{\"type\":\"object\",\"properties\":{\"sub\":{\"type\":\"long\"}}}", index),
            request(true, field("field3", "long"), index)
        );

        final ExecutionOutcome batched = execute(initial, true, requests);
        final ExecutionOutcome sequential = execute(initial, false, requests);

        assertThat(batched.taskResults(), equalTo(sequential.taskResults()));
        assertThat(batched.taskResults().get(0), equalTo("success"));
        assertThat(batched.taskResults().get(2), not(equalTo("success")));
        assertThat(batched.taskResults().get(3), equalTo("success"));
        assertThat(mappingSource(batched.state(), index), equalTo(mappingSource(sequential.state(), index)));
    }

    public void testNonAutoAndMultiIndexTasksAreBarriers() throws Exception {
        final Index index1 = createIndexWithExistingField("test1");
        final Index index2 = createIndexWithExistingField("test2");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();

        // a conflicting explicit update that only fails if the preceding auto update has already been applied
        final List<PutMappingClusterStateUpdateRequest> nonAuto = List.of(
            request(true, field("field1", "long"), index1),
            request(true, field("field2", "long"), index1),
            request(false, field("field1", "keyword"), index1),
            request(true, field("field3", "long"), index1)
        );
        ExecutionOutcome batched = execute(initial, true, nonAuto);
        ExecutionOutcome sequential = execute(initial, false, nonAuto);
        assertThat(batched.taskResults(), equalTo(sequential.taskResults()));
        assertThat(batched.taskResults().get(2), not(equalTo("success")));
        assertThat(mappingSource(batched.state(), index1), equalTo(mappingSource(sequential.state(), index1)));

        final List<PutMappingClusterStateUpdateRequest> multiIndex = List.of(
            request(true, field("field1", "long"), index2),
            request(true, field("field2", "long"), index2),
            request(false, field("field1", "keyword"), index1, index2),
            request(true, field("field3", "long"), index2)
        );
        batched = execute(initial, true, multiIndex);
        sequential = execute(initial, false, multiIndex);
        assertThat(batched.taskResults(), equalTo(sequential.taskResults()));
        assertThat(batched.taskResults().get(2), not(equalTo("success")));
        for (Index index : List.of(index1, index2)) {
            assertThat(mappingSource(batched.state(), index), equalTo(mappingSource(sequential.state(), index)));
        }
    }

    public void testGroupOfNoOpUpdatesChangesNothing() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final List<PutMappingClusterStateUpdateRequest> requests = List.of(
            request(true, field("existing", "keyword"), index),
            request(true, field("existing", "keyword"), index),
            request(true, field("existing", "keyword"), index)
        );

        final ExecutionOutcome outcome = execute(initial, true, requests);

        assertThat(outcome.taskResults(), equalTo(Collections.nCopies(3, "success")));
        assertSame(initial, outcome.state());
    }

    /**
     * A real {@link IndexSettingProvider} that records its invocations so the test can assert how often it is called and with what mapper.
     */
    private static class RecordingIndexSettingProvider implements IndexSettingProvider {
        final List<DocumentMapper> mappers = new ArrayList<>();

        @Override
        public void provideAdditionalSettings(
            String indexName,
            String dataStreamName,
            IndexMode templateIndexMode,
            boolean registryInstalledTemplate,
            ProjectMetadata projectMetadata,
            Instant resolvedAt,
            Settings indexTemplateAndCreateRequestSettings,
            List<CompressedXContent> combinedTemplateMappings,
            IndexVersion indexVersion,
            Settings.Builder additionalSettings
        ) {}

        @Override
        public void onUpdateMappings(IndexMetadata indexMetadata, DocumentMapper documentMapper, Settings.Builder additionalSettings) {
            mappers.add(documentMapper);
        }
    }

    public void testIndexSettingProviderIsInvokedOncePerChangedGroup() throws Exception {
        final Index index = createIndexWithExistingField("test");
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        final RecordingIndexSettingProvider provider = new RecordingIndexSettingProvider();
        final List<PutMappingClusterStateUpdateRequest> requests = List.of(
            request(true, field("field1", "long"), index),
            request(true, field("field2", "long"), index),
            request(true, field("field3", "long"), index)
        );

        final ExecutionOutcome outcome = execute(initial, true, new IndexSettingProviders(Set.of(provider)), requests);

        assertThat(outcome.taskResults(), equalTo(Collections.nCopies(3, "success")));
        assertThat(provider.mappers.size(), equalTo(1));
        assertThat(provider.mappers.get(0).mappingSource(), equalTo(mappingSource(outcome.state(), index)));
        assertThat(provider.mappers.get(0).mappers().getMapper("field3"), notNullValue());
    }

    public void testRandomizedBatchesMatchPerTaskExecution() throws Exception {
        final List<Index> indices = List.of(
            createIndexWithExistingField("test1"),
            createIndexWithExistingField("test2"),
            createIndexWithExistingField("test3")
        );
        final ClusterState initial = getInstanceFromNode(ClusterService.class).state();
        for (int iteration = 0; iteration < 10; iteration++) {
            final List<PutMappingClusterStateUpdateRequest> requests = new ArrayList<>();
            for (int i = randomIntBetween(1, 40); i > 0; i--) {
                final String properties = switch (randomIntBetween(0, 5)) {
                    case 0 -> "\"existing\":{\"type\":\"object\",\"properties\":{\"sub\":{\"type\":\"long\"}}}";
                    case 1 -> field("field" + randomIntBetween(0, 5) + ".sub", randomFrom("long", "keyword"));
                    case 2, 3 -> field("field" + randomIntBetween(0, 5), randomFrom("long", "keyword", "boolean"));
                    case 4 -> field("existing", randomFrom("keyword", "long"));
                    case 5 -> field("other" + randomIntBetween(0, 20), "long");
                    default -> throw new AssertionError();
                };
                final boolean autoUpdate = randomIntBetween(0, 9) != 0;
                if (randomIntBetween(0, 9) == 0) {
                    // multi-index tasks only add non-conflicting fields: a multi-index task that fails on a later index leaves the
                    // earlier indices' in-memory mapper services merged, so which later tasks see that leak depends on service reuse
                    final Index[] targets = randomSubsetOf(randomIntBetween(2, 3), indices).toArray(Index[]::new);
                    requests.add(request(autoUpdate, field("multi" + randomIntBetween(0, 5), "long"), targets));
                } else {
                    requests.add(request(autoUpdate, properties, randomFrom(indices)));
                }
            }

            final ExecutionOutcome batched = execute(initial, true, requests);
            final ExecutionOutcome sequential = execute(initial, false, requests);

            final String description = requests.stream()
                .map(r -> r.autoUpdate() + " " + Arrays.toString(r.indices()) + " " + r.source())
                .collect(Collectors.joining("\n"));
            assertThat(description, batched.taskResults(), equalTo(sequential.taskResults()));
            for (Index index : indices) {
                assertThat(description, mappingSource(batched.state(), index), equalTo(mappingSource(sequential.state(), index)));
            }
        }
    }

    private static List<PutMappingClusterStateUpdateTask> singleTask(PutMappingClusterStateUpdateRequest request) {
        return Collections.singletonList(new PutMappingClusterStateUpdateTask(request, ActionListener.running(() -> {
            throw new AssertionError("task should not complete publication");
        }), new HashMap<>()));
    }

}
