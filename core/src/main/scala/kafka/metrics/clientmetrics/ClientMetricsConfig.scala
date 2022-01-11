package kafka.metrics.clientmetrics

import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics.configDef
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.{INT, LIST, STRING}
import org.apache.kafka.common.errors.InvalidRequestException

import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Random

object ClientMetricsConfig {

  class SubscriptionGroup (subscriptionGroup: String,
                           subscribedMetrics: List[String],
                           clientMatchingPatterns: List[String],
                           pushIntervalMs: Int) {

    // TODO: Compute the checksum based on the current subscription
    def computeCheckSum :Long = {
      Random.nextLong()
    }
    val checkSum = computeCheckSum
    def getCheckSum = checkSum
    def getId = subscriptionGroup
    def getPushIntervalMs = pushIntervalMs
    def getClientMatchingPatterns = clientMatchingPatterns
    def getSubscribedMetrics = subscribedMetrics
  }

  object ClientMetrics {
    // Properties
    val SubscriptionGroupName = "client.metrics.subscription.group.name"
    val SubscriptionMetrics = "client.metrics.subscription.metrics"
    val ClientMatchPattern = "client.metrics.subscription.client.match"
    val PushIntervalMs = "client.metrics.push.interval.ms"

    // Definitions
    val configDef = new ConfigDef()
      .define(SubscriptionGroupName, STRING, MEDIUM, "Name of the metric subscription group")
      .define(SubscriptionMetrics, LIST, MEDIUM, "List of the subscribed metrics")
      .define(ClientMatchPattern, LIST, MEDIUM, "Pattern used to find the matching clients")
      .define(PushIntervalMs, INT, MEDIUM, "Interval that a client can push the metrics")

    def names = configDef.names

    private def validateParameter(parameter: String, logPrefix: String): Unit = {
      if (parameter.isEmpty) {
        throw new InvalidRequestException(logPrefix + " is illegal, it can't be empty")
      }
      if ("." == parameter || ".." == parameter) {
        throw new InvalidRequestException(logPrefix + " cannot be \".\" or \"..\"")
      }
    }

    def validate(name :String, properties :Properties): Unit = {
      validateParameter(name, "Client metrics subscription name")
      validateProperties(properties)
    }

    def validateProperties(properties :Properties) = {
      val names = configDef.names
      val propKeys = properties.keySet.asScala.map(_.asInstanceOf[String])
      val unknownProperties = propKeys.filter(!names.contains(_))
      require(unknownProperties.isEmpty, s"Unknown client metric configuration: $unknownProperties")
      require(properties.containsKey(ClientMetrics.ClientMatchPattern), s"Missing parameter ${ClientMatchPattern}")
      require(properties.containsKey(ClientMetrics.SubscriptionMetrics), s"Missing parameter ${SubscriptionMetrics}")
      require(properties.containsKey(ClientMetrics.PushIntervalMs), s"Missing parameter ${PushIntervalMs}")
    }
  }

  private val subscriptionMap = new ConcurrentHashMap[String, SubscriptionGroup]

  def getClientSubscriptionGroup(groupId :String): SubscriptionGroup  =  subscriptionMap.get(groupId)
  def clearClientSubscriptions(): Unit = subscriptionMap.clear
  def getSubscriptionGroupCount() = subscriptionMap.size

  private def toList(prop: Any): List[String] = {
    val value: util.List[_] = prop.asInstanceOf[util.List[_]]
    val valueList: util.ArrayList[String] = new util.ArrayList[String]
    value.forEach(x => valueList.add(x.asInstanceOf[String]))
    valueList.asScala.toList
  }

  def updateClientSubscription(groupId :String, properties: Properties): Unit = {
    val parsed = configDef.parse(properties)
    val metrics = toList(parsed.get(ClientMetrics.SubscriptionMetrics))
    val clientMatchPattern = toList(parsed.get(ClientMetrics.ClientMatchPattern))
    val pushInterval = parsed.get(ClientMetrics.PushIntervalMs).asInstanceOf[Int]

    if (metrics.size == 0 || metrics.mkString("").isEmpty) {
      subscriptionMap.remove(groupId)
    } else {
      subscriptionMap.put(groupId, new SubscriptionGroup(groupId, metrics, clientMatchPattern, pushInterval))
    }
  }

  def validateConfig(name :String, configs: Properties): Unit = ClientMetrics.validate(name, configs)
}
