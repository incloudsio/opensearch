/*
 * OpenSearch 1.3 side-car stub: stock FetchPhase has no ClusterService ctor or CQL hooks yet.
 * Full Elassandra CQL fetch lives in the ES 6.8 fork under org.elasticsearch.search.fetch.CqlFetchPhase.
 */
package org.opensearch.search.fetch;

import org.opensearch.cluster.service.ClusterService;

import java.util.List;

@SuppressWarnings("unused")
public class CqlFetchPhase extends FetchPhase {

    public static final String PROJECTION = "_projection";

    private final ClusterService clusterService;

    public CqlFetchPhase(List<FetchSubPhase> fetchSubPhases, ClusterService clusterService) {
        super(fetchSubPhases);
        this.clusterService = clusterService;
    }

    public ClusterService clusterService() {
        return clusterService;
    }
}
