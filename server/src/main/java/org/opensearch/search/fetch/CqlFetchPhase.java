package org.opensearch.search.fetch;

import org.opensearch.cluster.service.ClusterService;

import java.util.List;

/**
 * Elassandra: CQL-backed fetch (legacy ES path). OpenSearch 1.x {@link FetchPhase} has no CQL hooks;
 * this subclass preserves the type and {@link #PROJECTION} for callers until the port wires fetch properly.
 */
public class CqlFetchPhase extends FetchPhase {

    public static final String PROJECTION = "_projection";

    public CqlFetchPhase(List<FetchSubPhase> fetchSubPhases) {
        super(fetchSubPhases);
    }

    /** @deprecated OpenSearch {@link FetchPhase} does not take ClusterService; parameter ignored for DI compatibility. */
    @Deprecated
    public CqlFetchPhase(List<FetchSubPhase> fetchSubPhases, ClusterService clusterService) {
        this(fetchSubPhases);
    }
}
