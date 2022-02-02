package kafka.metrics

import kafka.metrics.clientmetrics.ClientMetricsConfig
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

import java.util.Properties

class ClientMetricsConfigValidationTest {

  @Test
  def testClientMetricsConfigParameters(): Unit = {
    val groupName: String = "subscription-1"
    val metrics = "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
    val clientMatchingPattern = "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538"

    val props = new Properties()

    // Test-1: Missing parameters (add one after one until all the required params are added)
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))

    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, 2000.toString)
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))

    props.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics)
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))

    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchingPattern)
    ClientMetricsConfig.validateConfig(groupName, props)

    props.put("INVALID_PARAMETER", "INVALID_ARGUMENT")
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))

    props.remove("INVALID_PARAMETER")
    ClientMetricsConfig.validateConfig(groupName, props)

    // TEST-2: Delete the metric subscription
    props.clear()
    props.put(ClientMetricsConfig.ClientMetrics.DeleteSubscription, "true")
    ClientMetricsConfig.validateConfig(groupName, props)

    // TEST-3: subscription with all metrics flag
    props.clear()
    props.put(ClientMetricsConfig.ClientMetrics.AllMetricsFlag, "true")
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchingPattern)
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, 2000.toString)
    ClientMetricsConfig.validateConfig(groupName, props)
  }

}
