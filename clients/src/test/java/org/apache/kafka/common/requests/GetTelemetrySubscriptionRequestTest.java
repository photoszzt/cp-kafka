/*
 Copyright 2021 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetTelemetrySubscriptionRequestTest {

    @Test
    public void testGetErrorResponse() {
        GetTelemetrySubscriptionRequest req = new GetTelemetrySubscriptionRequest(new GetTelemetrySubscriptionsRequestData(), (short) 0);
        GetTelemetrySubscriptionResponse response = req.getErrorResponse(0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 1), response.errorCounts());
    }

    @Test
    public void testErrorCountsReturnsNoneWhenNoErrors() {
        GetTelemetrySubscriptionsResponseData data = new GetTelemetrySubscriptionsResponseData()
                .setThrottleTimeMs(10)
                .setErrorCode(Errors.NONE.code());
        GetTelemetrySubscriptionResponse response = new GetTelemetrySubscriptionResponse(data);
        assertEquals(Collections.singletonMap(Errors.NONE, 1), response.errorCounts());
    }
    @Test
    public void testErrorCountsReturnsOneError() {
        GetTelemetrySubscriptionsResponseData data = new GetTelemetrySubscriptionsResponseData()
                .setThrottleTimeMs(10)
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        data.setErrorCode(Errors.INVALID_CONFIG.code());

        GetTelemetrySubscriptionResponse response = new GetTelemetrySubscriptionResponse(data);
        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertEquals(1, errorCounts.size());
        assertEquals(1, errorCounts.get(Errors.INVALID_CONFIG));
    }
}
