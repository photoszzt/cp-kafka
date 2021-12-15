package kafka.metrics.clientmetrics

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.{INT, LIST, STRING}
import org.apache.kafka.common.errors.InvalidRequestException

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
      require(properties.contains(ClientMetrics.ClientMatchPattern), s"Missing parameter ${ClientMatchPattern}")
      require(properties.contains(ClientMetrics.SubscriptionMetrics), s"Missing parameter ${SubscriptionMetrics}")
      require(properties.contains(ClientMetrics.PushIntervalMs), s"Missing parameter ${PushIntervalMs}")

      // Validate the property values
      configDef.parse(properties)
    }
  }

  private val subscriptionMap = new ConcurrentHashMap[String, SubscriptionGroup]

  def getClientSubscriptionGroup(groupId :String): SubscriptionGroup  =  subscriptionMap.get(groupId)
  def getSubscriptionGroupCount() = subscriptionMap.size()

  def createSubscriptionGroup(groupId :String, properties: Properties): Unit = {
    val parsedProperties = ClientMetrics.validateProperties(properties)
    val metrics = parsedProperties.get(ClientMetrics.SubscriptionMetrics).asInstanceOf[List[String]]
    val clientMatches = parsedProperties.get(ClientMetrics.ClientMatchPattern).asInstanceOf[List[String]]
    val pushInterval = parsedProperties.get(ClientMetrics.PushIntervalMs).asInstanceOf[String].toInt
    subscriptionMap.put(groupId,  new SubscriptionGroup(groupId, metrics, clientMatches, pushInterval))
  }

  def validateConfig(name :String, configs: Properties): Unit = ClientMetrics.validate(name, configs)

}
