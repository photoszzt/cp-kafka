package kafka.metrics.clientmetrics

import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionGroup
import org.apache.kafka.common.Uuid

import java.nio.charset.StandardCharsets
import java.util.Calendar
import java.util.zip.CRC32
import scala.collection.mutable.ListBuffer

object ClientInstanceState {

  def apply(entry: ClientInstanceState, sgroups: java.util.Collection[SubscriptionGroup]): ClientInstanceState = {
    var metrics = ListBuffer[String]()
    var pushInterval = 0
    sgroups.forEach(v =>
      if (entry.getClientSelector.isMatched(v.getClientMatchingPatterns)) {
        metrics = metrics ++ v.getSubscribedMetrics
        pushInterval = Math.min(pushInterval, v.getPushIntervalMs)
      }
    )
    new ClientInstanceState(entry.getId,  entry.getClientSelector, sgroups, metrics.toList, pushInterval)
  }

  def apply(id: Uuid, selector: ClientInstanceSelector, cmGroups: java.util.Collection[SubscriptionGroup]): ClientInstanceState = {
    var metrics = ListBuffer[String]()
    val sgroups = new java.util.ArrayList[SubscriptionGroup]()
    var pushInterval = 0
    cmGroups.forEach(v =>
      if (selector.isMatched(v.getClientMatchingPatterns)) {
        metrics = metrics ++ v.getSubscribedMetrics
        sgroups.add(v)
        pushInterval = Math.min(pushInterval, v.getPushIntervalMs)
      }
    )
    new ClientInstanceState(id, selector, sgroups, metrics.toList, pushInterval)
  }
}

class ClientInstanceState(clientInstanceId: Uuid,
                          selector: ClientInstanceSelector,
                          sgroups: java.util.Collection[SubscriptionGroup],
                          var metrics: List[String],
                          pushIntervalMs: Int) {

  private var metricsReceivedTs = getCurrentTime
  private val subscriptionId = computeSubscriptionId

  def getPushIntervalMs = pushIntervalMs
  def getLastMetricsReceivedTs = metricsReceivedTs
  def getSubscriptionId =  subscriptionId
  def getId = clientInstanceId
  def getCurrentTime = Calendar.getInstance.getTime
  def getClientSelector = selector
  def getSubscriptionGroups = sgroups

  // Computes the SubscriptionId as a unique identifier for a client instance's subscription set, the id is generated
  // by calculating a CRC32 of the configured metrics subscriptions including the PushIntervalMs,
  // XORed with the ClientInstanceId.
  private def computeSubscriptionId: Long = {
    val crc = new CRC32
    val metricsStr = metrics.toString() + pushIntervalMs.toString
    crc.update(metricsStr.getBytes(StandardCharsets.UTF_8))
    crc.getValue ^ clientInstanceId.hashCode
  }

  def updateMetricsReceivedTs(): Unit = {
    metricsReceivedTs = getCurrentTime
  }
}


