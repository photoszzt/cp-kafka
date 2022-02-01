package kafka.metrics.clientmetrics

import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics.{AllMetricsFlag, ClientMatchPattern, DeleteSubscription, PushIntervalMs, SubscriptionMetrics, configDef}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.{BOOLEAN, INT, LIST, STRING}
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidRequestException}

import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ClientMetricsConfig {

  class SubscriptionGroup(subscriptionGroup: String,
                          subscribedMetrics: List[String],
                          var matchingPatternsList: List[String],
                          pushIntervalMs: Int,
                          allMetricsSubscribed: Boolean = false) {
    def getId = subscriptionGroup
    def getPushIntervalMs = pushIntervalMs
    val clientMatchingPatterns = parseClientMatchingPatterns(matchingPatternsList)
    def getClientMatchingPatterns = clientMatchingPatterns
    def getSubscribedMetrics = subscribedMetrics
    def getAllMetricsSubscribed = allMetricsSubscribed

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
    val AllMetricsFlag = "client.metrics.all"
    val DeleteSubscription = "client.metrics.delete.subscription"

    val DEFAULT_PUSH_INTERVAL = 5 * 60 * 1000 // 5 minutes

    // Definitions
    val configDef = new ConfigDef()
      .define(SubscriptionGroupName, STRING, MEDIUM, "Name of the metric subscription group")
      .define(SubscriptionMetrics, LIST, MEDIUM, "List of the subscribed metrics")
      .define(ClientMatchPattern, LIST, MEDIUM, "Pattern used to find the matching clients")
      .define(PushIntervalMs, INT, DEFAULT_PUSH_INTERVAL, MEDIUM, "Interval that a client can push the metrics")
      .define(AllMetricsFlag, BOOLEAN, false, MEDIUM, "If set to true all the metrics are included")
      .define(DeleteSubscription, BOOLEAN, false, MEDIUM, "If set to true metric subscription would be deleted")

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
      require(properties.containsKey(SubscriptionGroupName), s"Missing parameter ${SubscriptionGroupName}")

      // If the command is to delete the subscription then we do not expect any other parameters to be in the list.
      // Otherwise validate the rest of the parameters.
      if (!properties.containsKey(DeleteSubscription)) {
        require(properties.containsKey(ClientMatchPattern), s"Missing parameter ${ClientMatchPattern}")
        require(properties.containsKey(PushIntervalMs), s"Missing parameter ${PushIntervalMs}")
        // If all metrics flag is specified then there is no need for having the metrics parameter
        if (!properties.containsKey(AllMetricsFlag)) {
          require(properties.containsKey(SubscriptionMetrics), s"Missing parameter ${SubscriptionMetrics}")
        }
      }
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
    val isDeleteSubscription = parsed.getOrDefault(DeleteSubscription, false).asInstanceOf[Boolean]
    if (isDeleteSubscription) {
      val deletedGroup = subscriptionMap.remove(groupId)
      cache.invalidate(deletedGroup, null, ClientMetricsCacheOperation.CM_SUBSCRIPTION_DELETED)
    } else {
      val clientMatchPattern = toList(parsed.get(ClientMatchPattern))
      val pushInterval = parsed.get(PushIntervalMs).asInstanceOf[Int]
      val allMetricsSubscribed = parsed.getOrDefault(AllMetricsFlag, false).asInstanceOf[Boolean]
      val metrics = if (allMetricsSubscribed) List("") else toList(parsed.get(SubscriptionMetrics))
      val newGroup = new SubscriptionGroup(groupId, metrics, clientMatchPattern, pushInterval, allMetricsSubscribed)
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
