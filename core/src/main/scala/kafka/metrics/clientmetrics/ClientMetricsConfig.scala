/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.metrics.clientmetrics

import kafka.metrics.clientmetrics.ClientMetricsCacheOperation.{CM_SUBSCRIPTION_ADDED, CM_SUBSCRIPTION_DELETED, CM_SUBSCRIPTION_UPDATED}
import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics.{AllMetricsFlag, ClientMatchPattern, DeleteSubscription, PushIntervalMs, SubscriptionMetrics, configDef}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.{BOOLEAN, INT, LIST}
import org.apache.kafka.common.errors.InvalidRequestException

import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer

/**
 * Client metric configuration related parameters and the supporting methods like validation and update methods
 * are defined in this class.
 * For more information please look at kip-714:
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability
 */
object ClientMetricsConfig {

  class SubscriptionGroup(subscriptionGroup: String,
                          subscribedMetrics: List[String],
                          var matchingPatternsList: List[String],
                          pushIntervalMs: Int,
                          allMetricsSubscribed: Boolean = false) {
    def getId = subscriptionGroup
    def getPushIntervalMs = pushIntervalMs
    val clientMatchingPatterns = CmClientInformation.parseMatchingPatterns(matchingPatternsList)
    def getClientMatchingPatterns = clientMatchingPatterns
    def getSubscribedMetrics = subscribedMetrics
    def getAllMetricsSubscribed = allMetricsSubscribed
  }

  object ClientMetrics {
    // Properties
    val SubscriptionMetrics = "client.metrics.subscription.metrics"
    val ClientMatchPattern = "client.metrics.subscription.client.match"
    val PushIntervalMs = "client.metrics.push.interval.ms"
    val AllMetricsFlag = "client.metrics.all"
    val DeleteSubscription = "client.metrics.delete.subscription"

    val DEFAULT_PUSH_INTERVAL = 5 * 60 * 1000 // 5 minutes

    // Definitions
    val configDef = new ConfigDef()
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

      // If the command is to delete the subscription then we do not expect any other parameters to be in the list.
      // Otherwise validate the rest of the parameters.
      if (!properties.containsKey(DeleteSubscription)) {
        require(properties.containsKey(PushIntervalMs), s"Missing parameter ${PushIntervalMs}")
        require(Integer.parseInt(properties.get(PushIntervalMs).toString) >= 0, s"Invalid parameter ${PushIntervalMs}")

        // If all metrics flag is specified then there is no need for having the metrics parameter
        if (!properties.containsKey(AllMetricsFlag)) {
          require(properties.containsKey(SubscriptionMetrics), s"Missing parameter ${SubscriptionMetrics}")
        }

        // Make sure that client match patterns are valid by parsing them.
        if (properties.containsKey(ClientMatchPattern)) {
          val propsList: List[String] = properties.getProperty(ClientMatchPattern).split(",").toList
          CmClientInformation.parseMatchingPatterns(propsList)
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
    val javaFalse = java.lang.Boolean.FALSE
    val subscriptionDeleted = parsed.getOrDefault(DeleteSubscription, javaFalse).asInstanceOf[Boolean]
    if (subscriptionDeleted) {
      val deletedGroup = subscriptionMap.remove(groupId)
      cache.invalidate(deletedGroup, null, CM_SUBSCRIPTION_DELETED)
    } else {
      val clientMatchPattern = toList(parsed.get(ClientMatchPattern))
      val pushInterval = parsed.get(PushIntervalMs).asInstanceOf[Int]
      val allMetricsSubscribed = parsed.getOrDefault(AllMetricsFlag, javaFalse).asInstanceOf[Boolean]
      val metrics = if (allMetricsSubscribed) List("") else toList(parsed.get(SubscriptionMetrics))
      val newGroup = new SubscriptionGroup(groupId, metrics, clientMatchPattern, pushInterval, allMetricsSubscribed)
      val oldGroup = subscriptionMap.put(groupId, newGroup)
      val operation = if (oldGroup != null) CM_SUBSCRIPTION_UPDATED else CM_SUBSCRIPTION_ADDED

      // Invalidate the the cache to absorb the changes from the new subscription group
      cache.invalidate(oldGroup, newGroup, operation)
    }
  }

  def validateConfig(name :String, configs: Properties): Unit = ClientMetrics.validate(name, configs)
}
