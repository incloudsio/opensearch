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

package org.elassandra.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.DiscoverySettings;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.EmptyTransportResponseHandler;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class AppliedClusterStateAction {

    private static final Logger logger = LogManager.getLogger(AppliedClusterStateAction.class);

    public static final String APPLIED_ACTION_NAME = "internal:discovery/cassandra/publish/applied";

    public interface AppliedClusterStateListener {
        /**
         * called when a cluster state has been committed and is ready to be processed
         */
        void onClusterStateApplied(String nodeId, String x2, Exception e, ActionListener<Void> processedListener);
    }

    private final TransportService transportService;
    private final AppliedClusterStateListener appliedClusterStateListener;
    private final DiscoverySettings discoverySettings;

    public AppliedClusterStateAction(
            Settings settings,
            TransportService transportService,
            AppliedClusterStateListener incomingClusterStateListener,
            DiscoverySettings discoverySettings) {
        this.transportService = transportService;
        this.appliedClusterStateListener = incomingClusterStateListener;
        this.discoverySettings = discoverySettings;
        transportService.registerRequestHandler(
            APPLIED_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            AppliedClusterStateRequest::new,
            new AppliedClusterStateRequestHandler()
        );
    }

    public void sendAppliedToNode(final DiscoveryNode node, final ClusterState clusterState, final Exception exception) {
        try {
            logger.trace("sending commit for cluster state [{}] to [{}]",
                clusterState.metadata().x2(), node);
            TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).build();
            // no need to put a timeout on the options here, because we want the response to eventually be received
            // and not log an error if it arrives after the timeout
            transportService.sendRequest(node, APPLIED_ACTION_NAME,
                    new AppliedClusterStateRequest(transportService.getLocalNode().getId(), clusterState.metadata().x2(), exception),
                    options,
                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            logger.debug("node {} responded to cluster state commit [{}]", node, clusterState.metadata().x2());
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug((org.apache.logging.log4j.util.Supplier<?>) () ->
                                new ParameterizedMessage("failed to commit cluster state [{}] to {}", clusterState.metadata().x2(), node), exp);
                        }
                    }
                    );
        } catch (Exception t) {
            logger.warn((org.apache.logging.log4j.util.Supplier<?>) () ->
                new ParameterizedMessage("error sending cluster state commit state [{}] to {}", clusterState.metadata().x2(), node), t);
        }
    }

    protected void handleAppliedRequest(AppliedClusterStateRequest request, final TransportChannel channel) {
        appliedClusterStateListener.onClusterStateApplied(request.nodeId, request.x2, request.e, new ActionListener<Void>() {

            @Override
            public void onResponse(Void ignore) {
                try {
                    // send a response to the master to indicate that this cluster state has been processed post committing it.
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } catch (Exception e) {
                    logger.debug("failed to send response on cluster state processed", e);
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.debug("failed to send response on cluster state processed", inner);
                }
            }
        });
    }

    private class AppliedClusterStateRequestHandler implements TransportRequestHandler<AppliedClusterStateRequest> {
        @Override
        public void messageReceived(AppliedClusterStateRequest request, final TransportChannel channel, Task task) throws Exception {
            handleAppliedRequest(request, channel);
        }
    }

    protected static class AppliedClusterStateRequest extends TransportRequest {
        String nodeId;
        String x2;
        Exception e;

        public AppliedClusterStateRequest() {
        }

        public AppliedClusterStateRequest(String nodeId, String x2, Exception e) {
            this.nodeId = nodeId;
            this.x2 = x2;
            this.e = e;
        }

        public AppliedClusterStateRequest(StreamInput in) throws IOException {
            super(in);
            nodeId = in.readString();
            x2 = in.readString();
            e = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(this.nodeId);
            out.writeString(this.x2);
            out.writeException(e);
        }
    }
}
