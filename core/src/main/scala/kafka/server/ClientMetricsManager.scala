package kafka.server

import kafka.metrics.clientmetrics.ClientMetricsConfig

import java.util.Properties

class ClientMetricsManager {

  def updateSubscription(groupId :String, properties :Properties) = {
    ClientMetricsConfig.updateClientSubscription(groupId, properties)
    invalidateCache();
  }

  def invalidateCache(): Unit = {

  }
}
