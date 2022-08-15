/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.net.InetAddress;
import java.util.List;


public class MetricsGrpcClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MetricsGrpcClient.class);

    public final StreamObserver<ExportMetricsServiceResponse> streamObserver =
            new StreamObserver<ExportMetricsServiceResponse>() {

        @Override
        public void onNext(ExportMetricsServiceResponse value) {
            // Do nothing since response is blank
        }

        @Override
        public void onError(Throwable t) {
            log.debug("Unable to export metrics request to {}. gRPC Connectivity in: {}:  {}",
                    endpoint, getChannelState(), t.getCause().getMessage());
        }

        @Override
        public void onCompleted() {
            log.info("Successfully exported metrics request to {}", endpoint);
        }
    };

    public static final String GRPC_HOST_CONFIG = "OTEL_EXPORTER_OTLP_ENDPOINT";

    public static final int DEFAULT_GRPC_PORT = 4317;

    public static final int GRPC_CHANNEL_TIMEOUT = 30;

    private MetricsServiceGrpc.MetricsServiceStub grpcClient;

    private ManagedChannel grpcChannel;

    private String endpoint;


    /**
     * Start a {@link ManagedChannel} and a {@link MetricsServiceGrpc} at an endpoint defined by the
     * {@value GRPC_HOST_CONFIG} environment variable
     */
    public void initialize() {
        if (grpcChannel == null) {
            String grpcHost = System.getenv(GRPC_HOST_CONFIG);

            if (Utils.isBlank(grpcHost)) {
                log.info("environment variable {} is not set", GRPC_HOST_CONFIG);

                try {
                    grpcHost = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    log.warn("Failed to get local host address: {}", e.getMessage());
                }
            }

            grpcChannel = ManagedChannelBuilder.forAddress(grpcHost, DEFAULT_GRPC_PORT).usePlaintext().build();
        }

        endpoint = grpcChannel.authority();
        grpcClient = MetricsServiceGrpc.newStub(grpcChannel);

        log.info("Started gRPC channel for endpoint  {}", endpoint);
    }


    /**
     * Exports a ExportMetricsServiceRequest to an external endpoint
     * @param resourceMetrics metrics to exports
     */
    public void export(List<ResourceMetrics> resourceMetrics) {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
                .addAllResourceMetrics(resourceMetrics)
                .build();

        grpcClient.export(request, streamObserver);
    }


    /**
     * Shuts down the gRPC {@link ManagedChannel}
     */
    @Override
    public void close()  {
        if(grpcChannel == null) {
            log.info("gRPC service is not initialize, no channel to shut down");
            return;
        }

        try {
            log.info("Shutting down gRPC channel at {}", endpoint);
            grpcChannel.shutdown().awaitTermination(GRPC_CHANNEL_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Failed Shutting down gRPC channel at {} : {}", endpoint, e.getMessage());
        }
    }


    public ConnectivityState getChannelState() {
        return grpcChannel.getState(true);
    }


    @VisibleForTesting
    public void setChannel(ManagedChannel channel) {
        grpcChannel = channel;
    }

    @VisibleForTesting
    public MetricsServiceGrpc.MetricsServiceStub getGrpcClient() {
        return grpcClient;
    }

    @VisibleForTesting
    public String getEndpoint() {
        return endpoint;
    }

    @VisibleForTesting
    public ManagedChannel getGrpcChannel() {
        return grpcChannel;
    }
}