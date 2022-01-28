package kafka.metrics.clientmetrics

import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics.configDef
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.{INT, LIST, STRING}
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidRequestException}

import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ClientMetricsConfig {

  class SubscriptionGroup (subscriptionGroup: String,
                           subscribedMetrics: List[String],
                           var matchingPatternsList: List[String],
                           pushIntervalMs: Int) {
    def getId = subscriptionGroup
    def getPushIntervalMs = pushIntervalMs
    val clientMatchingPatterns = parseClientMatchingPatterns(matchingPatternsList)
    def getClientMatchingPatterns = clientMatchingPatterns
    def getSubscribedMetrics = subscribedMetrics

    private def parseClientMatchingPatterns(patterns: List[String]) : Map[String, String] = {
      val patternsMap = mutable.Map[String, String]()
      patterns.foreach(x =>  {
        val nameValuePair = x.split("=").map(x => x.trim)
        if (nameValuePair.size != 2) {
          throw new InvalidConfigurationException("Illegal client matching pattern: " + x)
        }
        patternsMap += (nameValuePair(0) -> nameValuePair(1))
      })
      patternsMap.toMap
    }
  }

  object ClientMetrics {
    // Properties
    val SubscriptionGroupName = "client.metrics.subscription.group.name"
    val SubscriptionMetrics = "client.metrics.subscription.metrics"
    val ClientMatchPattern = "client.metrics.subscription.client.match"
    val PushIntervalMs = "client.metrics.push.interval.ms"
    val DEFAULT_PUSH_INTERVAL = 5 * 60 * 1000 // 5 minutes

    // Definitions
    val configDef = new ConfigDef()
      .define(SubscriptionGroupName, STRING, MEDIUM, "Name of the metric subscription group")
      .define(SubscriptionMetrics, LIST, MEDIUM, "List of the subscribed metrics")
      .define(ClientMatchPattern, LIST, MEDIUM, "Pattern used to find the matching clients")
      .define(PushIntervalMs, INT, DEFAULT_PUSH_INTERVAL, MEDIUM, "Interval that a client can push the metrics")

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
      properties.keySet().forEach(x => require(names.contains(x), s"Unknown client metric configuration: $x"))
      require(properties.containsKey(ClientMetrics.ClientMatchPattern), s"Missing parameter ${ClientMatchPattern}")
      require(properties.containsKey(ClientMetrics.SubscriptionMetrics), s"Missing parameter ${SubscriptionMetrics}")
      require(properties.containsKey(ClientMetrics.PushIntervalMs), s"Missing parameter ${PushIntervalMs}")
    }
  }

  private val subscriptionMap = new ConcurrentHashMap[String, SubscriptionGroup]

  def getClientSubscriptionGroup(groupId :String): SubscriptionGroup  =  subscriptionMap.get(groupId)
  def clearClientSubscriptions() = subscriptionMap.clear
  def getSubscriptionGroupCount = subscriptionMap.size
  def getClientSubscriptionGroups = subscriptionMap.values

  private def toList(prop: Any): List[String] = {
    val value: util.List[_] = prop.asInstanceOf[util.List[_]]
    val valueList =  new ListBuffer[String]()
    value.forEach(x => valueList += x.asInstanceOf[String])
    valueList.toList
  }

  def updateClientSubscription(groupId :String, properties: Properties, cache: ClientMetricsCache): Unit = {
    val parsed = configDef.parse(properties)
    val metrics = toList(parsed.get(ClientMetrics.SubscriptionMetrics))
    val clientMatchPattern = toList(parsed.get(ClientMetrics.ClientMatchPattern))
    val pushInterval = parsed.get(ClientMetrics.PushIntervalMs).asInstanceOf[Int]

    if (metrics.size == 0 || metrics.mkString("").isEmpty) {
      val sgroup = subscriptionMap.remove(groupId)
      cache.invalidate(sgroup, null, ClientMetricsCacheOperation.CM_SUBSCRIPTION_DELETED)
    } else {
      val newGroup = new SubscriptionGroup(groupId, metrics, clientMatchPattern, pushInterval)
      val oldGroup = subscriptionMap.put(groupId, newGroup)
      if (oldGroup != null) {
        cache.invalidate(oldGroup, newGroup, ClientMetricsCacheOperation.CM_SUBSCRIPTION_UPDATED)
      } else {
        cache.invalidate(oldGroup, newGroup, ClientMetricsCacheOperation.CM_SUBSCRIPTION_ADDED)
      }
    }
  }

  def validateConfig(name :String, configs: Properties): Unit = ClientMetrics.validate(name, configs)
}
