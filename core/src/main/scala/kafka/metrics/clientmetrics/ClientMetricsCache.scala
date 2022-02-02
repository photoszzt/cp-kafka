package kafka.metrics.clientmetrics

import kafka.Kafka.info
import kafka.metrics.clientmetrics.ClientMetricsCache.isExpired
import kafka.metrics.clientmetrics.ClientMetricsCacheOperation.{CM_SUBSCRIPTION_ADDED, CM_SUBSCRIPTION_DELETED, CM_SUBSCRIPTION_TTL, CM_SUBSCRIPTION_UPDATED, ClientMetricsCacheOperation}
import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionGroup
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidRequestException

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
 *      which is is an asynchronous task triggered by ClientMetricsManager.
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
            info(s"Client Metrics subscription cache cleanup failed ${exception.getMessage}")
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
    cacheInstance.invalidate(null, null, CM_SUBSCRIPTION_TTL)
    preCleanupSize - cacheInstance.getSize
  }
}

class ClientMetricsCache {
  val _cache = new ConcurrentHashMap[String, CmClientInstanceState]()
  def getSize = _cache.size()
  def add(instance: CmClientInstanceState)= _cache.put(instance.getId.toString, instance)
  def get(id: Uuid): CmClientInstanceState = _cache.get(id.toString)
  def clear() = _cache.clear()

  // Invalidates the client metrics cache by iterating through all the client instances and do one of the following:
  //     1. TTL operation -- Cleans up all the cache entries that are expired beyond TTL allowed time.
  //     2. Adding a new group -- Update the metrics by appending the new metrics from the new group
  //     3. Deleting an existing group -- Update the metrics by deleting the metrics from the deleted group.
  //     4. Updating an existing group. -- delete the old metrics and add the new metrics.
  def invalidate(oldGroup: SubscriptionGroup,
                 newGroup: SubscriptionGroup,
                 operation: ClientMetricsCacheOperation) = {
    operation match {
      case CM_SUBSCRIPTION_TTL => cleanupTtlEntries()
      case CM_SUBSCRIPTION_ADDED => addCMSubscriptionGroup(newGroup)
      case CM_SUBSCRIPTION_DELETED => deleteCmSubscriptionGroup(oldGroup)
      case CM_SUBSCRIPTION_UPDATED => updateCmSubscriptionGroup(oldGroup, newGroup)
      case _ => throw new InvalidRequestException("Invalid request, can not update the client metrics cache")
    }
  }

  // Updates the client instance objects
  private def addCMSubscriptionGroup(cmSubscriptionGroup: SubscriptionGroup) = {
    val affectedElements = new ListBuffer[CmClientInstanceState] ()
    _cache.values().forEach(v =>
        if (v.getClientInfo.isMatched(cmSubscriptionGroup.getClientMatchingPatterns)) {
          v.getSubscriptionGroups.add(cmSubscriptionGroup)
          affectedElements.append(CmClientInstanceState(v))
        }
    )
    affectedElements.foreach(x => _cache.replace(x.getId.toString, x))
  }

  private def deleteCmSubscriptionGroup(cmSubscriptionGroup: SubscriptionGroup) = {
    val affectedElements = new ListBuffer[CmClientInstanceState] ()
    _cache.values().forEach(v =>
      if (v.getClientInfo.isMatched(cmSubscriptionGroup.getClientMatchingPatterns)) {
        v.getSubscriptionGroups.remove(cmSubscriptionGroup)
        if (!v.getSubscriptionGroups.isEmpty)
          affectedElements.append(CmClientInstanceState(v))
      }
    )
    affectedElements.foreach(x => _cache.replace(x.getId.toString, x))
  }

  private def updateCmSubscriptionGroup(oldGroup: SubscriptionGroup, newGroup: SubscriptionGroup) = {
    val affectedElements = new ListBuffer[CmClientInstanceState] ()
    _cache.values().forEach(v => {
      var changed: Boolean = false
      if (v.getClientInfo.isMatched(oldGroup.getClientMatchingPatterns)) {
        changed = true
        v.getSubscriptionGroups.remove(oldGroup)
      }
      if (v.getClientInfo.isMatched(newGroup.getClientMatchingPatterns)) {
        changed = true
        v.getSubscriptionGroups.add(newGroup)
      }
      if (!v.getSubscriptionGroups.isEmpty && changed) {
          affectedElements.append(CmClientInstanceState(v))
      }
    })

    affectedElements.foreach(x => _cache.replace(x.getId.toString, x))
  }

  /**
   * Client instance specific state is maintained in broker memory up to MAX(60*1000, PushIntervalMs * 3) milliseconds
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
      info(s"Client subscription entry ${x.getId.toString} is expired removing it from the cache")
      _cache.remove(x.getId.toString)
    })
  }


}
