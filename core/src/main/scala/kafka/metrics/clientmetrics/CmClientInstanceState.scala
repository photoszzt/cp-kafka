package kafka.metrics.clientmetrics

import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics.DEFAULT_PUSH_INTERVAL
import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionGroup
import org.apache.kafka.common.Uuid

import java.nio.charset.StandardCharsets
import java.util.Calendar
import java.util.zip.CRC32
import scala.collection.mutable.ListBuffer

object CmClientInstanceState {

  def apply(entry: CmClientInstanceState, sgroups: java.util.Collection[SubscriptionGroup]): CmClientInstanceState = {
    var metrics = new ListBuffer[String]()
    var pushInterval = DEFAULT_PUSH_INTERVAL
    sgroups.forEach(v =>
      if (entry.getClientInfo.isMatched(v.getClientMatchingPatterns)) {
        metrics = metrics ++ v.getSubscribedMetrics
        pushInterval = Math.min(pushInterval, v.getPushIntervalMs)
      }
    )
    new CmClientInstanceState(entry.getId,  entry.getClientInfo, sgroups, metrics.toList, pushInterval)
  }

  def apply(id: Uuid, clientInfo: CmClientInformation, cmGroups: java.util.Collection[SubscriptionGroup]): CmClientInstanceState = {
    var metrics = new ListBuffer[String]()
    val sgroups = new java.util.ArrayList[SubscriptionGroup]()
    var pushInterval = DEFAULT_PUSH_INTERVAL
    cmGroups.forEach(v =>
      if (clientInfo.isMatched(v.getClientMatchingPatterns)) {
        metrics = metrics ++ v.getSubscribedMetrics
        sgroups.add(v)
        pushInterval = Math.min(pushInterval, v.getPushIntervalMs)
      }
    )
    // TODO: what happens when there are no matching groups.
    new CmClientInstanceState(id, clientInfo, sgroups, metrics.toList, pushInterval)
  }
}

class CmClientInstanceState(clientInstanceId: Uuid,
                            clientInfo: CmClientInformation,
                            sgroups: java.util.Collection[SubscriptionGroup],
                            var metrics: List[String],
                            pushIntervalMs: Int) {

  private val metricsReceivedTs = Calendar.getInstance.getTime
  private val subscriptionId = computeSubscriptionId

  def getPushIntervalMs = pushIntervalMs
  def getLastMetricsReceivedTs = metricsReceivedTs
  def getSubscriptionId =  subscriptionId
  def getId = clientInstanceId
  def getClientInfo = clientInfo
  def getSubscriptionGroups = sgroups
  def getMetrics = metrics

  def updateMetricsReceivedTs(tsInMs: Long): Unit =  {
    metricsReceivedTs.setTime(tsInMs)
  }

  // Computes the SubscriptionId as a unique identifier for a client instance's subscription set, the id is generated
  // by calculating a CRC32 of the configured metrics subscriptions including the PushIntervalMs,
  // XORed with the ClientInstanceId.
  private def computeSubscriptionId: Long = {
    val crc = new CRC32
    val metricsStr = metrics.toString() + pushIntervalMs.toString
    crc.update(metricsStr.getBytes(StandardCharsets.UTF_8))
    crc.getValue ^ clientInstanceId.hashCode
  }

}


