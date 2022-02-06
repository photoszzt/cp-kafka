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
import kafka.metrics.clientmetrics.ClientMetricsCache.isExpired
import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionGroup
import org.apache.kafka.common.Uuid
import org.apache.log4j.helpers.LogLog.error

import java.util.Calendar
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * Allowed operations on the ClientMetricsCache
 */
object ClientMetricsCacheOperation extends Enumeration {
  type ClientMetricsCacheOperation = Value
  val CM_SUBSCRIPTION_ADDED, CM_SUBSCRIPTION_DELETED, CM_SUBSCRIPTION_UPDATED, CM_SUBSCRIPTION_TTL = Value
}

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
  val gcTs = Calendar.getInstance.getTime

  private val cacheInstance = new ClientMetricsCache
  def getInstance = cacheInstance
  val getSize = getInstance.getSize

  /**
   * Launches the asynchronous task to clean the client metric subscriptions that are expired in the cache.
   * TODO: in future, if needed, we may need to run this task periodically regardless of the size of the cache
   */
  def runGC()=  {
    gcTs.synchronized {
      val currentTime = Calendar.getInstance.getTime
      if (currentTime.getTime - gcTs.getTime > CM_CACHE_GC_INTERVAL) {
        gcTs.setTime(currentTime.getTime)
        cleanupExpiredEntries("GC").onComplete {
          case Success(value) => info(s"Client Metrics subscriptions cache cleaned up $value entries")
          case Failure(exception) =>
            error(s"Client Metrics subscription cache cleanup failed: ${exception.getMessage}")
        }
      }
    }
  }

  private def isExpired(element: CmClientInstanceState) = {
    val currentTs = Calendar.getInstance.getTime
    val delta = currentTs.getTime - element.getLastMetricsReceivedTs.getTime
    delta > Math.max(3 * element.getPushIntervalMs, DEFAULT_TTL_MS)
  }

  private def cleanupExpiredEntries(reason: String): Future[Int] = Future {
    val preCleanupSize = cacheInstance.getSize
    cacheInstance.cleanupTtlEntries()
    preCleanupSize - cacheInstance.getSize
  }
}

class ClientMetricsCache {
  val _cache = new ConcurrentHashMap[Uuid, CmClientInstanceState]()
  def getSize = _cache.size()
  def add(instance: CmClientInstanceState)= _cache.put(instance.getId, instance)
  def get(id: Uuid): CmClientInstanceState = _cache.get(id)
  def clear() = _cache.clear()

  /**
   * Updates the client metric instance state objects that matches with the group that is being replaced.
   * Since old subscription group and new subscription group may have different metrics and client match
   * patterns hence find out all the elements that do the following:
   *   - matches with oldGroup: delete the old group from the client instance
   *   - matches with newGroup: add the new group to the client instance.
   *  Finally, creates a new instance with the updated subscription groups and replace the old one.
   * @param olGroup -- The group that is been deleted
   * @param newGroup -- New group that has been added
   */
  def update(olGroup: SubscriptionGroup, newGroup: SubscriptionGroup) = {
    val iter = _cache.keys()
    while (iter.hasMoreElements) {
      val v = _cache.get(iter.nextElement())
      if (updateCacheElement(v, olGroup, newGroup)) {
        _cache.replace(v.getId, CmClientInstanceState(v))
      }
    }
  }

  private def updateCacheElement(instance: CmClientInstanceState,
                                 oldGroup: SubscriptionGroup,
                                 newGroup: SubscriptionGroup) = {
    var changed: Boolean = false
    if (oldGroup!= null && instance.getClientInfo.isMatched(oldGroup.getClientMatchingPatterns)) {
      changed = true
      instance.getSubscriptionGroups.remove(oldGroup)
    }

    if (newGroup != null && instance.getClientInfo.isMatched(newGroup.getClientMatchingPatterns)) {
      changed = true
      instance.getSubscriptionGroups.add(newGroup)
    }
    changed
  }

  /**
   * Client instance state is maintained in broker memory up to Max(60*1000, PushIntervalMs * 3) milliseconds
   * There is no persistence of client instance metrics state across the broker restarts or between brokers.
   */
  private def cleanupTtlEntries() = {
    val expiredElements = new ListBuffer[CmClientInstanceState] ()
    _cache.values().forEach(x =>  {
      if (isExpired(x)) {
        expiredElements.append(x)
      }
    })
    expiredElements.foreach( x => {
      info(s"Client subscription entry ${x.getId} is expired removing it from the cache")
      _cache.remove(x.getId)
    })
  }

}
