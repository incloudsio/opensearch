/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains some code from Elasticsearch (http://www.elastic.co)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elassandra;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.StorageService;
import org.opensearch.cli.MockTerminal;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.Index;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.translog.TruncateTranslogAction;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Elassandra snapshot tests.
 * @author vroyer
 *
 */
//mvn test -Pdev -pl om.strapdata.elasticsearch:elasticsearch -Dtests.seed=622A2B0618CE4676 -Dtests.class=org.elassandra.SnapshotTests -Des.logger.level=ERROR -Dtests.assertion.disabled=false -Dtests.security.manager=false -Dtests.heap.size=1024m -Dtests.locale=ro-RO -Dtests.timezone=America/Toronto
public class SnapshotTests extends OpenSearchSingleNodeTestCase {
    private static final int SNAPSHOT_DOC_COUNT = 64;

    private String randomKeyspaceName(String prefix) {
        return (prefix + "_" + randomAlphaOfLength(8)).toLowerCase(Locale.ROOT);
    }

    private Settings snapshotIndexSettings() {
        return Settings.builder()
            .put("index.snapshot_with_sstable", true)
            .put("index.synchronous_refresh", false)
            .build();
    }

    private void assertSearchHitCount(String index, String type, long expected) throws Exception {
        assertBusy(() -> {
            try {
                assertThat(
                    client().prepareSearch().setIndices(index).setTypes(type).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value,
                    equalTo(expected)
                );
            } catch (SearchPhaseExecutionException e) {
                throw new AssertionError("search shards are not ready yet", e);
            }
        });
    }

    private void deleteAllRows(String keyspace, String table, long count) throws Exception {
        for (long i = 0; i < count; i++) {
            process(ConsistencyLevel.ONE, String.format(Locale.ROOT, "DELETE FROM %s.%s WHERE name = 'name%d'", keyspace, table, i));
        }
    }

    private void refreshIndex(String index) {
        client().admin().indices().prepareRefresh(index).get();
    }

    private void waitForStartedPrimaryShard(Index index) throws Exception {
        assertBusy(() -> {
            final IndexShard shard = clusterService().indexServiceSafe(index).getShardOrNull(0);
            assertNotNull(shard);
            assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        });
    }

    private Path luceneSnapshotRoot(Index index) {
        return clusterService().indexServiceSafe(index).getShardOrNull(0).shardPath().resolveSnapshot().resolve(index.getUUID());
    }

    private Path luceneIndexPath(Index index) {
        return clusterService().indexServiceSafe(index).getShardOrNull(0).shardPath().resolveIndex();
    }

    private ShardPath shardPath(Index index) {
        return clusterService().indexServiceSafe(index).getShardOrNull(0).shardPath();
    }

    private void rebuildTranslogFromRestoredCommit(ShardPath shardPath) throws Exception {
        final TruncateTranslogAction action = new TruncateTranslogAction(getInstanceFromNode(NamedXContentRegistry.class));
        try (FSDirectory directory = FSDirectory.open(shardPath.resolveIndex())) {
            action.execute(new MockTerminal(), shardPath, directory);
        }
    }

    // SSTable snapshotDir = data/<keyspace>/<table>/snapshots/<snapshot_name>/
    public void restoreSSTable(String dataLocation, String keyspaceName, String cfName, UUID srcId, UUID dstId, String snapshotName) throws IOException {
        Path sourceDir = PathUtils.get(dataLocation+"/"+keyspaceName+"/"+cfName+"-"+srcId.toString().replaceAll("\\-", "")+"/snapshots/"+snapshotName);
        Path targetDir = PathUtils.get(dataLocation+"/"+keyspaceName+"/"+cfName+"-"+dstId.toString().replaceAll("\\-", "")+"/");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(targetDir, "{*.db}")) {
            for (Path dbFile: stream)
                Files.delete(dbFile);
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceDir)) {
            for (Path f: stream) {
                System.out.println("cp "+f+" "+targetDir.toString());
                Files.copy(f, PathUtils.get(targetDir.toString(), f.getFileName().toString()) , StandardCopyOption.COPY_ATTRIBUTES);
            }
        }
        System.out.println();
    }

    // lucene snapshotDir = data/elasticsearch.data/nodes/0/snapshots/<index_uuid>/<snapshot_name>
    // index dir = data/elasticsearch.data/nodes/0/indices/<index_uuid>/0/index/
    public void restoreLucenceFiles(Path sourceDir, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(targetDir, "{_*.*,segments*}")) {
            for (Path segmentFile: stream)
                Files.delete(segmentFile);
        }
        try (FSDirectory sourceIndex = FSDirectory.open(sourceDir)) {
            final java.util.List<IndexCommit> commits = DirectoryReader.listCommits(sourceIndex);
            final IndexCommit latestCommit = commits.get(commits.size() - 1);
            for (String fileName : latestCommit.getFileNames()) {
                Path sourceFile = sourceDir.resolve(fileName);
                System.out.println("cp "+sourceFile+" "+targetDir.toString());
                Files.copy(sourceFile, targetDir.resolve(fileName), StandardCopyOption.COPY_ATTRIBUTES);
            }
        }
    }

    @Test
    public void basicSnapshotTest() throws Exception {
        final String keyspace = randomKeyspaceName("ks");
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': '1'}", keyspace, DatabaseDescriptor.getLocalDataCenter()));
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE TABLE %s.t1 ( name text, age int, primary key (name))", keyspace));

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("t1").field("discover", ".*").endObject().endObject();
        createIndex(keyspace, snapshotIndexSettings(),"t1", mapping);
        ensureGreen(keyspace);
        Index initialIndex = resolveIndex(keyspace);
        waitForStartedPrimaryShard(initialIndex);
        UUID srcCfId = Schema.instance.getTableMetadata(keyspace, "t1").id.asUUID();
        Path luceneSnapshotRoot = luceneSnapshotRoot(initialIndex);

        for(long i=0; i < SNAPSHOT_DOC_COUNT; i++)
           process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "INSERT INTO %s.t1 (name, age) VALUES ('name%d', %d)", keyspace, i, i));
        refreshIndex(keyspace);
        assertSearchHitCount(keyspace, "t1", SNAPSHOT_DOC_COUNT);

        // take snaphot
        StorageService.instance.takeSnapshot("snap1", keyspace);

        // Recreate the schema before restore so the explicit snapshot is applied onto a fresh shard history.
        assertAcked(client().admin().indices().prepareDelete(keyspace).get());
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': '1'}", keyspace, DatabaseDescriptor.getLocalDataCenter()));
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE TABLE %s.t1 ( name text, age int, primary key (name))", keyspace));
        createIndex(keyspace, snapshotIndexSettings(),"t1", mapping);
        ensureGreen(keyspace);
        Index restoredIndex = resolveIndex(keyspace);
        waitForStartedPrimaryShard(restoredIndex);
        Path restoredLuceneIndexPath = luceneIndexPath(restoredIndex);
        ShardPath restoredShardPath = shardPath(restoredIndex);

        assertSearchHitCount(keyspace, "t1", 0L);
        assertAcked(client().admin().indices().prepareClose(keyspace).get());

        String dataLocation = DatabaseDescriptor.getAllDataFileLocations()[0];
        restoreSSTable(dataLocation, keyspace, "t1", srcCfId, Schema.instance.getTableMetadata(keyspace, "t1").id.asUUID(), "snap1");
        restoreLucenceFiles(luceneSnapshotRoot.resolve("snap1"), restoredLuceneIndexPath);
        rebuildTranslogFromRestoredCommit(restoredShardPath);

        // refresh SSTables and repopen index
        StorageService.instance.loadNewSSTables(keyspace, "t1");
        assertAcked(client().admin().indices().prepareOpen(keyspace).get());
        ensureGreen(keyspace);
        waitForStartedPrimaryShard(resolveIndex(keyspace));

        assertSearchHitCount(keyspace, "t1", SNAPSHOT_DOC_COUNT);
        assertAcked(client().admin().indices().prepareDelete(keyspace).get());
    }

    @Test
    //mvn test -Pdev -pl org.opensearch:elasticsearch -Dtests.seed=622A2B0618CE4676 -Dtests.class=org.elassandra.SnapshotTests -Dtests.method="onDropSnapshotTest" -Des.logger.level=ERROR -Dtests.assertion.disabled=false -Dtests.security.manager=false -Dtests.heap.size=1024m -Dtests.locale=ro-RO -Dtests.timezone=America/Toronto
    public void onDropSnapshotTest() throws Exception {
        final String keyspace = randomKeyspaceName("ks");
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': '1'}", keyspace, DatabaseDescriptor.getLocalDataCenter()));
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE TABLE %s.t1 ( name text, age int, primary key (name))", keyspace));

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("t1").field("discover", ".*").endObject().endObject();
        createIndex(keyspace, snapshotIndexSettings(),"t1", mapping);
        ensureGreen(keyspace);
        Index index1 = resolveIndex(keyspace);
        waitForStartedPrimaryShard(index1);
        Path luceneSourceSnapshotRoot = luceneSnapshotRoot(index1);

        for(long i=0; i < SNAPSHOT_DOC_COUNT; i++)
           process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "INSERT INTO %s.t1 (name, age) VALUES ('name%d', %d)", keyspace, i, i));
        refreshIndex(keyspace);
        assertSearchHitCount(keyspace, "t1", SNAPSHOT_DOC_COUNT);

        UUID cfId = Schema.instance.getTableMetadata(keyspace,"t1").id.asUUID();
        String id = cfId.toString().replaceAll("\\-", "");

        if (!DatabaseDescriptor.isAutoSnapshot())
            StorageService.instance.takeTableSnapshot(keyspace, "t1", Long.toString(new Date().getTime()));

        // drop index + keyspace (C* snapshot before drop => flush before snapshot => ES flush before delete)
        assertAcked(client().admin().indices().prepareDelete(keyspace).get());

        // recreate schema and mapping
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': '1'}", keyspace, DatabaseDescriptor.getLocalDataCenter()));
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE TABLE %s.t1 ( name text, age int, primary key (name))", keyspace));
        createIndex(keyspace, snapshotIndexSettings(),"t1", mapping);
        ensureGreen(keyspace);
        Index index2 = resolveIndex(keyspace);
        waitForStartedPrimaryShard(index2);
        Path luceneTargetIndexPath = luceneIndexPath(index2);
        ShardPath targetShardPath = shardPath(index2);

        assertSearchHitCount(keyspace, "t1", 0L);

        // close index and restore SSTable+Lucene files
        assertAcked(client().admin().indices().prepareClose(keyspace).get());

        String dataLocation = DatabaseDescriptor.getAllDataFileLocations()[0];
        DirectoryStream<Path> stream = Files.newDirectoryStream(PathUtils.get(dataLocation+"/"+keyspace+"/t1-"+id+"/snapshots/"));
        Path snapshot = stream.iterator().next();
        String snap = snapshot.getFileName().toString();
        System.out.println("snapshot name="+snap);
        stream.close();

        UUID cfId2 = Schema.instance.getTableMetadata(keyspace,"t1").id.asUUID();
        restoreSSTable(dataLocation, keyspace, "t1", cfId, cfId2, snap);
        restoreLucenceFiles(luceneSourceSnapshotRoot.resolve(snap), luceneTargetIndexPath);
        rebuildTranslogFromRestoredCommit(targetShardPath);

        // refresh SSTables and repopen index
        StorageService.instance.loadNewSSTables(keyspace, "t1");
        assertAcked(client().admin().indices().prepareOpen(keyspace).get());
        ensureGreen(keyspace);
        waitForStartedPrimaryShard(resolveIndex(keyspace));

        Thread.sleep(3000);
        assertSearchHitCount(keyspace, "t1", SNAPSHOT_DOC_COUNT);
        assertAcked(client().admin().indices().prepareDelete(keyspace).get());
    }

    @Test
    public void keepDataOnDelete() throws Exception {
        final String keyspace = randomKeyspaceName("ks");
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': '1'}", keyspace, DatabaseDescriptor.getLocalDataCenter()));
        process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "CREATE TABLE %s.t1 ( name text, age int, primary key (name))", keyspace));

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("t1").field("discover", ".*").endObject().endObject();
        createIndex(
            keyspace,
            Settings.builder().put("index.drop_on_delete_index", false).put("index.synchronous_refresh", false).build(),
            "t1",
            mapping
        );
        ensureGreen(keyspace);
        waitForStartedPrimaryShard(resolveIndex(keyspace));

        int N = 10;
        for(long i=0; i < N; i++)
           process(ConsistencyLevel.ONE,String.format(Locale.ROOT, "INSERT INTO %s.t1 (name, age) VALUES ('name%d', %d)", keyspace, i, i));
        refreshIndex(keyspace);
        assertSearchHitCount(keyspace, "t1", N);

        assertAcked(client().admin().indices().prepareDelete(keyspace).get());
        UntypedResultSet rs = process(ConsistencyLevel.ONE, String.format(Locale.ROOT, "SELECT * FROM %s.t1", keyspace));
        assertThat(rs.size(), equalTo(N));

        process(ConsistencyLevel.ONE, String.format(Locale.ROOT, "DROP KEYSPACE %s", keyspace));
    }
}
