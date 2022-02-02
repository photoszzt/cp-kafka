package kafka.metrics

import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionGroup
import kafka.metrics.clientmetrics.{ClientMetricsConfig, CmClientInformation, CmClientInstanceState}
import kafka.server.ClientMetricsManager
import org.apache.kafka.common.Uuid

import java.util.Properties

object ClientMetricsTestUtils {
  val defaultPushInterval = 30 * 1000 // 30 seconds

  val defaultMetrics =
    "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"

  val defaultClientMatchPatters =
    List(s"${CmClientInformation.CLIENT_SOFTWARE_NAME}=Java", s"${CmClientInformation.CLIENT_SOFTWARE_VERSION}=11.1.*")

  val cmInstance :ClientMetricsManager = ClientMetricsManager.getInstance
  def getCM = cmInstance

  def updateClientSubscription(subscriptionId :String, properties: Properties): Unit = {
    getCM.updateSubscription(subscriptionId, properties)
  }

  def createClientInstance(selector: CmClientInformation): CmClientInstanceState = {
    getCM.createClientInstance(Uuid.randomUuid(), selector)
  }


  def getDefaultProperties() :Properties = {
    val props = new Properties()
    props.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, defaultMetrics)
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, defaultClientMatchPatters.mkString(","))
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, defaultPushInterval.toString)
    props
  }

  def createCMSubscriptionGroup(groupName: String, overrideProps: Properties = null): SubscriptionGroup = {
    val props = getDefaultProperties()
    if (overrideProps != null) {
      overrideProps.entrySet().forEach(x => props.put(x.getKey, x.getValue))
    }
    updateClientSubscription(groupName, props)
    ClientMetricsConfig.getClientSubscriptionGroup(groupName)
  }
}
