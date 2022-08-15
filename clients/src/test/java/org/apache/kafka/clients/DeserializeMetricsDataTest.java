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
package org.apache.kafka.clients;

import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.Gauge;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeserializeMetricsDataTest {

    public ByteBuffer metricsDataBuffer;
    public MetricsData metricsData;
    public Metric sampleMetric;


    @BeforeEach
    void setUp() {
        NumberDataPoint.Builder point = NumberDataPoint.newBuilder().setAsInt(15);
        Gauge gauge = Gauge.newBuilder().addDataPoints(point).build();
        MetricsData.Builder builder = MetricsData.newBuilder();

        sampleMetric = Metric.newBuilder()
                .setName("Gauge-Test-Metric")
                .setGauge(gauge)
                .build();


        ResourceMetrics rm = ResourceMetrics.newBuilder()
                .addInstrumentationLibraryMetrics(
                        InstrumentationLibraryMetrics.newBuilder()
                                .addMetrics(sampleMetric)
                                .build()
                ).build();

        builder.addResourceMetrics(rm);
        metricsData = builder.build();

        metricsDataBuffer = ByteBuffer.allocate(1024).put(metricsData.toByteArray());
    }

    @Test
    public void testDeserializeMetricsData() {
        MetricsData deserializedMetricsData = ClientTelemetryUtils.deserializeMetricsData(metricsDataBuffer);
        InstrumentationLibraryMetrics instLib = deserializedMetricsData.getResourceMetrics(0)
                .getInstrumentationLibraryMetrics(0);
        Metric metric = instLib.getMetrics(0);

        assertNotNull(deserializedMetricsData);
        assertEquals(metricsData, deserializedMetricsData);
        assertEquals(1, instLib.getMetricsCount());
        assertEquals(sampleMetric, metric);
    }
}
