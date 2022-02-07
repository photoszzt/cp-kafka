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

import kafka.Kafka.info
import kafka.metrics.clientmetrics.ClientMetricsCache.{DEFAULT_TTL_MS, cmCache}
import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionGroup
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.cache.LRUCache
import org.apache.log4j.helpers.LogLog.error

import java.util.Calendar
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * Client Metrics Cache:
 *   Cache of the client instance state objects that are created in response to the client's
 *   GetTelemetrySubscriptionRequest message.
 *
 *   Eviction Policy:
 *      Elements stayed too long (beyond the TTL time period) would be cleaned up from the cache by running GC
 *      which is an asynchronous task triggered by ClientMetricsManager.
 *
 *   Invalidation (keeping the client instance state in sync with client metric subscriptions):
 *      Whenever a client metric subscription is added/deleted/modified cache is invalidated to make sure that
 *      cached elements reflects the changes made to the client metric subscriptions.
 */
object  ClientMetricsCache {
  val DEFAULT_TTL_MS = 60 * 1000  // One minute
  val CM_CACHE_GC_INTERVAL = 5 * 60 * 1000 // 5 minutes
  val CM_CACHE_MAX_SIZE = 8192 // What would be the right cache size?
  val gcTs = Calendar.getInstance.getTime
  private val cmCache = new ClientMetricsCache(CM_CACHE_MAX_SIZE)

  def getInstance = cmCache

  /**
   * Launches the asynchronous task to clean the client metric subscriptions that are expired in the cache.
   */
  def runGCIfNeeded(forceGC: Boolean = false): Unit = {
    gcTs.synchronized {
      val timeElapsed = Calendar.getInstance.getTime.getTime - gcTs.getTime
      if (forceGC || cmCache.getSize > CM_CACHE_MAX_SIZE && timeElapsed > CM_CACHE_GC_INTERVAL) {
        cmCache.cleanupExpiredEntries("GC").onComplete {
          case Success(value) => info(s"Client Metrics subscriptions cache cleaned up $value entries")
          case Failure(e) => error(s"Client Metrics subscription cache cleanup failed: ${e.getMessage}")
        }
      }
    }
  }

}

class ClientMetricsCache(maxSize: Int) {
  private val _cache = new LRUCache[Uuid, ClientMetricsCacheValue](maxSize)
  def getSize = _cache.size()
  def clear() = _cache.clear()
  def get(id: Uuid): CmClientInstanceState =  {
    val value = _cache.get(id)
    if (value != null) value.getClientInstance else null
  }

  /**
   * Iterates through all the elements of the cache and updates the client instance state objects that
   * matches the group that is being updated.
   * @param oldGroup -- Subscription group that has been deleted from the client metrics subscription
   * @param newGroup -- subscription group that has been added to the client metrics subscription
   */
  def invalidate(oldGroup: SubscriptionGroup, newGroup: SubscriptionGroup) = {
    update(oldGroup, newGroup)
  }

  ///////// **** PRIVATE - METHODS **** /////////////
  def add(instance: CmClientInstanceState)= {
    _cache.synchronized(_cache.put(instance.getId, new ClientMetricsCacheValue(instance)))
  }

  private def remove(id: Uuid) : Unit = {
    _cache.synchronized(_cache.remove(id))
  }

  private def updateValue(element: ClientMetricsCacheValue, instance: CmClientInstanceState)= {
    _cache.synchronized(element.setClientInstance(instance))
  }

  private def update(oldGroup: SubscriptionGroup, newGroup: SubscriptionGroup) = {
    _cache.entrySet().forEach(element =>  {
      val clientInstance = element.getValue.getClientInstance
      val updatedMetricSubscriptions = clientInstance.getSubscriptionGroups
      if (oldGroup!= null && clientInstance.getClientInfo.isMatched(oldGroup.getClientMatchingPatterns)) {
        updatedMetricSubscriptions.remove(oldGroup)
      }
      if (newGroup != null && clientInstance.getClientInfo.isMatched(newGroup.getClientMatchingPatterns)) {
        updatedMetricSubscriptions.add(newGroup)
      }
      val newClientInstance = CmClientInstanceState(clientInstance, updatedMetricSubscriptions)
      updateValue(element.getValue, newClientInstance)
    })
  }

  private def isExpired(element: CmClientInstanceState) = {
    val currentTs = Calendar.getInstance.getTime
    val delta = currentTs.getTime - element.getLastMetricsReceivedTs.getTime
    delta > Math.max(3 * element.getPushIntervalMs, DEFAULT_TTL_MS)
  }

  private def cleanupExpiredEntries(reason: String): Future[Long] = Future {
    val preCleanupSize = cmCache.getSize
    cmCache.cleanupTtlEntries()
    preCleanupSize - cmCache.getSize
  }

  private def cleanupTtlEntries() = {
    val expiredElements = new ListBuffer[Uuid] ()
    _cache.entrySet().forEach(x => {
      if (isExpired(x.getValue.clientInstance)) {
        expiredElements.append(x.getValue.getClientInstance.getId)
      }
    })
    expiredElements.foreach(x => {
      info(s"Client subscription entry ${x} is expired removing it from the cache")
      remove(x)
    })
  }

  // Wrapper class
  class ClientMetricsCacheValue(instance: CmClientInstanceState) {
    var clientInstance :CmClientInstanceState = instance
    def getClientInstance = clientInstance
    def setClientInstance(instance: CmClientInstanceState): Unit = {
      clientInstance = instance
    }
  }
}
