package kafka.metrics

import kafka.metrics.ClientMetricsTestUtils.{createCMSubscriptionGroup, getCM}
import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics
import kafka.metrics.clientmetrics.{ClientMetricsConfig, CmClientInformation}
import kafka.server.ClientMetricsManager
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterEach, Test}

import java.util.Properties

class ClientMetricsRequestResponseTest {

  @AfterEach
  def cleanup(): Unit = {
    ClientMetricsConfig.clearClientSubscriptions()
    getCM.clearCache()
  }

  @Test def testGetClientMetricsRequestAndResponse(): Unit = {
    val sgroup1 = createCMSubscriptionGroup("cm_1")
    assertTrue(sgroup1 != null)

    val requestData = new GetTelemetrySubscriptionsRequestData()
    val request = new GetTelemetrySubscriptionRequest(requestData, 0)
    assertTrue(request.getClientInstanceId == Uuid.ZERO_UUID)

    val clientInfo = CmClientInformation("testClient1", "clientId3", "Java", "11.1.0", "192.168.1.7", "9093")
    val response = getCM.processGetSubscriptionRequest(request, clientInfo, 20)
    val responseData = response.data()
    assertTrue(response != null && responseData != null)

   // verify all the parameters ..
    assertTrue(responseData.clientInstanceId() != Uuid.ZERO_UUID)
    val cmClient = getCM.getClientInstance(responseData.clientInstanceId())
    assertTrue(cmClient != null)

    assertTrue(response.throttleTimeMs() == 20)
    assertTrue(responseData.deltaTemporality() == true)
    assertTrue(responseData.pushIntervalMs() == cmClient.getPushIntervalMs)
    assertTrue(responseData.subscriptionId() == cmClient.getSubscriptionId)

    assertTrue(responseData.requestedMetrics().size == cmClient.getMetrics.size)
    responseData.requestedMetrics().forEach(x => assertTrue(cmClient.getMetrics.contains(x)))

    assertTrue(responseData.acceptedCompressionTypes().size() == ClientMetricsManager.getSupportedCompressionTypes.size)
    responseData.acceptedCompressionTypes().forEach(x =>
      assertTrue(ClientMetricsManager.getSupportedCompressionTypes.contains(x)))
  }

  @Test def testRequestAndResponseWithNoMatchingMetrics(): Unit = {
    val sgroup1 = createCMSubscriptionGroup("cm_2")
    assertTrue(sgroup1 != null)

    val request = new GetTelemetrySubscriptionRequest(new GetTelemetrySubscriptionsRequestData(), 0)
    assertTrue(request.getClientInstanceId == Uuid.ZERO_UUID)

    // Create a python client that do not have any matching subscriptions.
    val clientInfo = CmClientInformation("testClient1", "clientId3", "Python", "11.1.0", "192.168.1.7", "9093")
    var response = getCM.processGetSubscriptionRequest(request, clientInfo, 20).data()
    var cmClient = getCM.getClientInstance(response.clientInstanceId())

    // Push interval must be set to the default push interval.
    assertTrue(response.pushIntervalMs() == ClientMetrics.DEFAULT_PUSH_INTERVAL &&
               response.pushIntervalMs() != sgroup1.getPushIntervalMs)
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)
    assertTrue(response.requestedMetrics().isEmpty)

    // Now create a client subscription with client id that matched with the client.
    val props = new Properties()
    val clientMatch = List(s"${CmClientInformation.CLIENT_SOFTWARE_NAME}=Python",
                           s"${CmClientInformation.CLIENT_SOFTWARE_VERSION}=11.1.*")
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatch.mkString(","))
    val sgroup2 = createCMSubscriptionGroup("cm_2", props)
    assertTrue(sgroup2 != null)

    // should have got the positive response with all the valid parameters
    response = getCM.processGetSubscriptionRequest(request, clientInfo, 20).data()
    cmClient = getCM.getClientInstance(response.clientInstanceId())
    assertTrue(response.pushIntervalMs() == sgroup2.getPushIntervalMs)
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)
    assertTrue(!response.requestedMetrics().isEmpty)
    response.requestedMetrics().forEach(x => assertTrue(cmClient.getMetrics.contains(x)))
  }

  @Test def testRequestWithAllMetrics(): Unit = {
    /*

    // Using the empty metrics list can be confusing, it is causing the confusion
    // about the semantics of removing subscription vs subscription for all metrics
    // TODO: Needs to confirm the right behavior with Magnus

    val props = new Properties
    props.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, "")
    val sgroup1 = createCMSubscriptionGroup("cm_all_metrics", props)
    assertTrue(sgroup1 != null)

    val requestData = new GetTelemetrySubscriptionsRequestData()
    val request = new GetTelemetrySubscriptionRequest(requestData, 0)
    assertTrue(request.getClientInstanceId == Uuid.ZERO_UUID)

    val clientInfo = CmClientInformation("testClient1", "clientId3", "Java", "11.1.0", "192.168.1.7", "9093")
    val response = getCM.processGetSubscriptionRequest(request, clientInfo, 20).data()

    // verify all the parameters ..
    assertTrue(response.clientInstanceId() != Uuid.ZERO_UUID)
    val cmClient = getCM.getClientInstance(response.clientInstanceId())
    assertTrue(cmClient != null)

    assertTrue(response.pushIntervalMs() == cmClient.getPushIntervalMs)
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)

     */
  }

  @Test def testRequestWithDisabledClient(): Unit = {
    val sgroup1 = createCMSubscriptionGroup("cm_4")
    assertTrue(sgroup1 != null)

    val request = new GetTelemetrySubscriptionRequest(new GetTelemetrySubscriptionsRequestData(), 0)
    assertTrue(request.getClientInstanceId == Uuid.ZERO_UUID)

    // Create a python client that do not have any matching subscriptions.
    val clientInfo = CmClientInformation("testClient5", "clientId5", "Java", "11.1.0", "192.168.1.7", "9093")
    var response = getCM.processGetSubscriptionRequest(request, clientInfo, 20).data()
    var cmClient = getCM.getClientInstance(response.clientInstanceId())

    // Push interval must be set to the default push interval.
    assertTrue(response.pushIntervalMs() == sgroup1.getPushIntervalMs)
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)
    response.requestedMetrics().forEach(x => assertTrue(cmClient.getMetrics.contains(x)))


    // Now create a new client subscription with push interval set to 0.
    val props = new Properties()
    props.put(ClientMetrics.PushIntervalMs,0.toString)
    val sgroup2 = createCMSubscriptionGroup("cm_update_2_disable", props)
    assertTrue(sgroup2 != null)

    // should have got the invalid response with empty metrics list.
    val res = getCM.processGetSubscriptionRequest(request, clientInfo, 20)
    assertTrue(res.error() == Errors.INVALID_CONFIG)
    cmClient = getCM.getClientInstance(response.clientInstanceId())
    response = res.data()
    assertTrue(response.pushIntervalMs() == 0)
    assertTrue(response.requestedMetrics().isEmpty)
  }


}
