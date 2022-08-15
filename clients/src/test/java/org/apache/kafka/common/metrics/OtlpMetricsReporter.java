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

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.resource.v1.Resource;

import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.clients.ClientTelemetryUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * MetricsReporter that aggregates Opentelemetry Protocol(OTLP) metrics, enhance them with additional client labels
 * and forwards them via gRPC Client to an external OTLP receiver.
 */
public class OtlpMetricsReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(OtlpMetricsReporter.class);

    private MetricsGrpcClient grpcService;

    public boolean appendClientLabels = true;

    public static final String APPEND_CLIENT_LABELS = "APPEND_CLIENT_LABELS";

    // Kafka-specific labels
    public Map<String, String> metricsContext;
    public static final String KAFKA_BROKER_ID = "kafka.broker.id";
    public static final String KAFKA_CLUSTER_ID = "kafka.cluster.id";

    public OtlpMetricsReporter() {
        grpcService = new MetricsGrpcClient();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object clientLabelsConfig = configs.get(APPEND_CLIENT_LABELS);
        if (clientLabelsConfig != null) {
            appendClientLabels = (boolean) clientLabelsConfig;
        }

        grpcService.initialize();
    }


    @Override
    public void contextChange(MetricsContext metricsContext) {
        this.metricsContext = metricsContext.contextLabels();
    }


    @Override
    public void reconfigure(Map<String, ?> configs) {
        MetricsReporter.super.reconfigure(configs);
    }

    @Override
    public void close() {
        grpcService.close();
    }

    @Override
    public ClientTelemetryReceiver clientReceiver() {
        return (context, payload) -> {

            if (payload == null || payload.data() == null) {
                log.warn("exportMetrics - Client did not include payload when pushing to broker, suspended export");
                return;
            }

            MetricsData metricsData = ClientTelemetryUtils.deserializeMetricsData(payload.data());
            if (metricsData == null || metricsData.getResourceMetricsCount() == 0) {
                log.warn("exportMetrics - No metrics available, suspended export");
                return;
            }

            List<ResourceMetrics> metrics = metricsData.getResourceMetricsList();

            // enhance metrics with Broker-added labels
            if (appendClientLabels) {
                Map<String, String> labels = getClientLabels(context, payload);
                metrics = enhanceMetrics(metrics, labels);
            }

            //grpcService.export(metrics);
            log.info("exportMetrics - metrics {}", metrics);
        };
    }


    /**
     * Returns a list of ResourceMetrics where each metrics is enhanced by adding client information as
     * additional resource level attributes
     * @param resourceMetrics resource metrics sent by client
     * @param labels broker added labels containing client information
     * @return enhanced ResourceMetrics list
     */
    public List<ResourceMetrics> enhanceMetrics(List<ResourceMetrics> resourceMetrics,
                                                Map<String, String> labels) {

        List<ResourceMetrics> updatedResourceMetrics = new ArrayList<>();

        resourceMetrics.forEach(rm -> {
            Resource.Builder resource = rm.getResource().toBuilder();

            labels.forEach((k, v) -> resource.addAttributes(
                    KeyValue.newBuilder()
                            .setKey(k)
                            .setValue(AnyValue.newBuilder().setStringValue(v))
                            .build()
                    ));

            ResourceMetrics updatedMetric = rm.toBuilder()
                    .setResource(resource.build())
                    .build();

            updatedResourceMetrics.add(updatedMetric);
        });

        return updatedResourceMetrics;
    }

    public static class ClientLabels {

        public static final String CLIENT_ID = "client.id";
        public static final String CLIENT_INSTANCE_ID = "client.instance.id";
        public static final String CLIENT_SOFTWARE_NAME = "client.software.name";
        public static final String CLIENT_SOFTWARE_VERSION = "client.software.version";
        public static final String CLIENT_SOURCE_ADDRESS = "client.source.address";
        public static final String PRINCIPAL = "principal";
    }

    public Map<String, String> getClientLabels(AuthorizableRequestContext context, ClientTelemetryPayload payload) {
        RequestContext requestContext = (RequestContext) context;

        Map<String, String> labels = new HashMap<>();
        putIfNotNull(labels, ClientLabels.CLIENT_ID, context.clientId());
        putIfNotNull(labels, ClientLabels.CLIENT_INSTANCE_ID, payload.clientInstanceId().toString());
        putIfNotNull(labels, ClientLabels.CLIENT_SOFTWARE_NAME, requestContext.clientInformation.softwareName());
        putIfNotNull(labels, ClientLabels.CLIENT_SOFTWARE_VERSION, requestContext.clientInformation.softwareVersion());
        putIfNotNull(labels, ClientLabels.CLIENT_SOURCE_ADDRESS, requestContext.clientAddress().getHostAddress());
        putIfNotNull(labels, ClientLabels.PRINCIPAL, requestContext.principal().getName());


        // Include Kafka cluster and broker id from the MetricsContext
        putIfNotNull(labels, KAFKA_CLUSTER_ID, metricsContext.get(KAFKA_CLUSTER_ID));
        putIfNotNull(labels, KAFKA_BROKER_ID, metricsContext.get(KAFKA_BROKER_ID));

        return labels;
    }

    public static void putIfNotNull(Map<String, String> map, String key, String value) {
        Optional.ofNullable(value).ifPresent(v -> map.put(key, v));
    }

    @Override
    public void init(List<KafkaMetric> metrics) {}

    @Override
    public void metricChange(KafkaMetric metric) {}

    @Override
    public void metricRemoval(KafkaMetric metric) {}

    @VisibleForTesting
    public MetricsGrpcClient getGrpcService() {
        return grpcService;
    }
}