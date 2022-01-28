package kafka.metrics

import kafka.metrics.clientmetrics.ClientMetricsCache.DEFAULT_TTL_MS
import kafka.metrics.clientmetrics.{ClientMetricsCache, ClientMetricsConfig, CmClientInformation, CmClientInstanceState}
import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionGroup
import kafka.server.ClientMetricsManager
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterEach, Test}

import java.util.Properties

class ClientMetricsCacheTest {
  val pushInterval = 30 * 1000 // 30 seconds
  val metrics = "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
  val patternsList = List(s"${CmClientInformation.CLIENT_SOFTWARE_NAME}=Java", s"${CmClientInformation.CLIENT_SOFTWARE_VERSION}=11.1.*")

  private def updateClientSubscription(subscriptionId :String, properties: Properties): Unit = {
    ClientMetricsManager.get.updateSubscription(subscriptionId, properties)
  }

  private def createClientInstance(selector: CmClientInformation): CmClientInstanceState = {
    ClientMetricsManager.get.createClientInstance(Uuid.randomUuid(), selector)
  }

  private def createCMSubscriptionGroup(groupName: String, overrideProps: Properties = null): SubscriptionGroup = {
    val props = new Properties()
    props.put(ClientMetricsConfig.ClientMetrics.SubscriptionGroupName, groupName)
    props.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics)
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, patternsList.mkString(","))
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, pushInterval.toString)
    if (overrideProps != null) {
      overrideProps.entrySet().forEach(x => props.put(x.getKey, x.getValue))
    }
    updateClientSubscription(groupName, props)
    ClientMetricsConfig.getClientSubscriptionGroup(groupName)
  }

  @AfterEach
  def cleanup(): Unit = {
    ClientMetricsConfig.clearClientSubscriptions()
    ClientMetricsManager.get.clearCache()
  }

  @Test
  def testClientMetricsGroup(): Unit = {
    // create a client metric subscription group.
    val sgroup1 = createCMSubscriptionGroup("cm_1")
    assertTrue(sgroup1 != null)

    // create a client instance state object and make sure it picks up the metrics from the previously created
    // metrics subscription group.
    val client = CmClientInformation("testClient1", "clientId", "Java", "11.1.0.1", "", "")
    val clientState = createClientInstance(client)
    val clientStateFromCache = ClientMetricsManager.get.getClientInstance(clientState.getId)
    assertTrue(clientState == clientStateFromCache)
    assertTrue(clientStateFromCache.getSubscriptionGroups.size() == 1)
    assertTrue(clientStateFromCache.getPushIntervalMs == pushInterval)
    assertTrue(clientStateFromCache.metrics.size == 2 && clientStateFromCache.getMetrics.mkString(",").equals(metrics))
  }

  @Test
  def testAddingGroupsAfterClients(): Unit = {
    // create a client instance state object  when there are no client metrics subscriptions exists
    val client = CmClientInformation("testClient1", "clientId", "Java", "11.1.0.1", "", "")
    val clientState = createClientInstance(client)
    var clientStateFromCache = ClientMetricsManager.get.getClientInstance(clientState.getId)
    assertTrue(clientState == clientStateFromCache)
    assertTrue(clientStateFromCache.getSubscriptionGroups.isEmpty)
    assertTrue(clientStateFromCache.getMetrics.isEmpty)
    val oldSubscriptionId = clientStateFromCache.getSubscriptionId

    // Now create a new client subscription and make sure the client instance is updated with the metrics.
    createCMSubscriptionGroup("cm_1")
    clientStateFromCache = ClientMetricsManager.get.getClientInstance(clientState.getId)
    assertTrue(clientStateFromCache.getSubscriptionGroups.size() == 1)
    assertTrue(clientStateFromCache.getPushIntervalMs == pushInterval)
    assertTrue(clientStateFromCache.getSubscriptionId != oldSubscriptionId)
    assertTrue(clientStateFromCache.metrics.size == 2 && clientStateFromCache.getMetrics.mkString(",").equals(metrics))
  }

  @Test
  def testAddingMultipleSubscriptionGroups(): Unit = {
    val props = new Properties()
    val clientMatchPatterns = List(s"${CmClientInformation.CLIENT_SOFTWARE_NAME}=Java", s"${CmClientInformation.CLIENT_SOFTWARE_VERSION}=8.1.*")
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchPatterns.mkString(","))

    // TEST-1: CREATE new metric subscriptions and make sure client instance picks up those metrics.
    val sgroup1 = createCMSubscriptionGroup("cm_1")
    val sgroup2 = createCMSubscriptionGroup("cm_2", props)
    assertTrue(sgroup1 != null && sgroup2 != null)

    // create a client instance state object and make sure every thing is in correct order.
    val client = CmClientInformation("testClient1", "clientId", "Java", "11.1.0.1", "", "")
    val clientState = createClientInstance(client)
    val clientStateFromCache = ClientMetricsManager.get.getClientInstance(clientState.getId)
    assertTrue(clientState == clientStateFromCache)

    val res = clientState.getSubscriptionGroups
    assertTrue(res.size() ==1)
    assertTrue(res.contains(sgroup1))
    assertTrue(clientState.getPushIntervalMs == pushInterval)
    assertTrue(clientState.metrics.size == 2 && clientState.metrics.mkString(",").equals(metrics))

    // TEST-2: UPDATE the metrics subscription: Create update the metrics subscriptions by adding new subscription group
    // with different metrics and make sure that client instance object is updated with the new metric and
    // new subscription id is computed
    val metrics3 = "org.apache.kafka/client.producer.write.latency"
    val props3 = new Properties()
    props3.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics3)
    createCMSubscriptionGroup("cm_3", props3)
    val afterAddingNewGroup = ClientMetricsManager.get.getClientInstance(clientState.getId)
    assertTrue(clientStateFromCache.getId == afterAddingNewGroup.getId)
    assertTrue(clientState.getSubscriptionId != afterAddingNewGroup.getSubscriptionId)
    assertTrue(afterAddingNewGroup.metrics.size == 3 && afterAddingNewGroup.metrics.mkString(",").equals(metrics + "," + metrics3))

    // TEST-3: UPDATE the first group metrics and make sure client instance picked up the change.
    val updated_metrics = "updated_metrics_for_clients"
    val updatedProps = new Properties()
    updatedProps.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, updated_metrics)
    createCMSubscriptionGroup("cm_1", updatedProps)
    val afterSecondUpdate = ClientMetricsManager.get.getClientInstance(clientState.getId)
    assertTrue(afterSecondUpdate.getId == afterAddingNewGroup.getId)
    assertTrue(afterSecondUpdate.getSubscriptionId != afterAddingNewGroup.getSubscriptionId)
    assertTrue(afterSecondUpdate.metrics.size == 2 && afterSecondUpdate.metrics.mkString(",").equals(metrics3 + "," + updated_metrics))

    // TEST3: DELETE the metrics subscription: Delete the first group and make sure client instance is updated
    val props4 = new Properties()
    props4.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, "")
    createCMSubscriptionGroup("cm_1", props4)
    val afterDeletingGroup = ClientMetricsManager.get.getClientInstance(clientState.getId)
    assertTrue(afterAddingNewGroup.getId == afterDeletingGroup.getId)
    assertTrue(afterAddingNewGroup.getSubscriptionId != afterDeletingGroup.getSubscriptionId)
    assertTrue(afterDeletingGroup.metrics.size == 1 && afterDeletingGroup.metrics.mkString(",").equals(metrics3))
  }

  @Test
  def testMultipleClientsAndGroups(): Unit = {
    createCMSubscriptionGroup("cm_1")

    val metrics2 = "org.apache.kafka/client.producer.write.latency"
    val props2 = new Properties()
    props2.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics2)
    createCMSubscriptionGroup("cm_2", props2)

    val props3 = new Properties()
    val clientPatterns3 = List(s"${CmClientInformation.CLIENT_SOFTWARE_NAME}=Python", s"${CmClientInformation.CLIENT_SOFTWARE_VERSION}=8.*")
    val metrics3 = "org.apache.kafka/client.consumer.read.latency"
    props3.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics3)
    props3.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientPatterns3.mkString(","))
    createCMSubscriptionGroup("cm_3", props3)

    val props4 = new Properties()
    val clientPatterns4 = List(s"${CmClientInformation.CLIENT_SOFTWARE_NAME}=Python",
      s"${CmClientInformation.CLIENT_SOFTWARE_VERSION}=8.*",s"${CmClientInformation.CLIENT_SOURCE_ADDRESS} = 1.2.3.4")
    val metrics4 = "org.apache.kafka/client.consumer.*.latency"
    props4.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientPatterns4.mkString(","))
    props4.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics4)
    createCMSubscriptionGroup("cm_4", props4)
    assertTrue(ClientMetricsConfig.getSubscriptionGroupCount == 4)

    val cache = ClientMetricsCache.getInstance
    val client1 = createClientInstance(CmClientInformation("testClient1", "clientId1", "Java", "11.1.0.1", "", ""))
    val client2 = createClientInstance(CmClientInformation("testClient2", "clientId2", "Python", "8.2.1", "abcd", "0"))
    val client3 = createClientInstance(CmClientInformation("testClient3", "clientId3", "C++", "12.1", "192.168.1.7", "9093"))
    val client4 = createClientInstance(CmClientInformation("testClient4", "clientId4", "Java", "11.1", "1.2.3.4", "8080"))
    val client5 = createClientInstance(CmClientInformation("testClient2", "clientId5", "Python", "8.2.1", "1.2.3.4", "0"))
    assertTrue(cache.getSize == 5)

    // Verifications:
    // Client 1 should have the metrics from the groups sgroup1 and sgroup2
    assertTrue(client1.getMetrics.mkString(",").equals(metrics + "," + metrics2))

    // Client 2 should have the group3 which is just default metrics
    assertTrue(client2.getMetrics.mkString(",").equals(metrics3))

    // client 3 should end up with nothing.
    assertTrue(client3.getMetrics.isEmpty)

    // Client 4 should have the metrics from the groups sgroup1 and sgroup2
    assertTrue(client4.getMetrics.mkString(",").equals(metrics + "," + metrics2))

    // Client 5 should have the metrics from the group sgroup3 and sgroup4
    assertTrue(client5.getMetrics.mkString(",").equals(metrics3 + "," + metrics4))
  }


  @Test
  def testCacheGC(): Unit = {
    val cache = ClientMetricsCache.getInstance
    val client1 = createClientInstance(CmClientInformation("testClient1", "clientId1", "Java", "11.1.0.1", "", ""))
    val client2 = createClientInstance(CmClientInformation("testClient2", "clientId2", "Python", "8.2.1", "", ""))
    val client3 = createClientInstance(CmClientInformation("testClient3", "clientId3", "C++", "12.1", "", ""))
    assertTrue(cache.getSize == 3)

    val delta =  client3.getLastMetricsReceivedTs.getTime - (Math.max(3 * client3.getPushIntervalMs, DEFAULT_TTL_MS) + 1000)
    client3.updateMetricsReceivedTs(delta)
    ClientMetricsCache.gcTs.setTime(ClientMetricsCache.gcTs.getTime - (ClientMetricsCache.CM_CACHE_GC_INTERVAL + 1000))
    ClientMetricsCache.runGC()

    // Wait until GC removed the client3 entry from the cache
    TestUtils.waitUntilTrue(() => ClientMetricsCache.getInstance.getSize == 2,  "Failed to run GC on Client Metrics Cache", 6 * 1000)
    assertTrue(cache.get(client1.getId) != null)
    assertTrue(cache.get(client2.getId) != null)
    assertTrue(cache.get(client3.getId) == null)
  }

}
