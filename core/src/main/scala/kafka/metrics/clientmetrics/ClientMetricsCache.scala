package kafka.metrics.clientmetrics

import kafka.Kafka.info
import kafka.metrics.clientmetrics.ClientMetricsCacheOperation.{CM_SUBSCRIPTION_ADDED, CM_SUBSCRIPTION_DELETED, CM_SUBSCRIPTION_TTL, CM_SUBSCRIPTION_UPDATED, ClientMetricsCacheOperation}
import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionGroup
import org.apache.kafka.common.errors.InvalidRequestException

import java.util.Calendar
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object ClientMetricsCacheOperation extends Enumeration {
  type ClientMetricsCacheOperation = Value
  val CM_SUBSCRIPTION_ADDED, CM_SUBSCRIPTION_DELETED, CM_SUBSCRIPTION_UPDATED, CM_SUBSCRIPTION_TTL = Value
}

class ClientMetricsCache {
  val _cache = new ConcurrentHashMap[String, ClientInstanceState]()
  var gcTs = Calendar.getInstance.getTime
  val DEFAULT_TTL_MS = 60 * 1000  // One minute
  val CM_CACHE_GC_INTERVAL = 3 * DEFAULT_TTL_MS
  val CM_CACHE_MAX_SIZE = 1024

  def put(instance: ClientInstanceState): Unit = {
    _cache.put(instance.getId.toString, instance)

    // if cache size > max entries then time to make some room.
    // TODO: in future, if needed, we may need to run this task periodically regardless of the size of the cache
    if (_cache.size >  CM_CACHE_MAX_SIZE)
      runCmCacheCleaner("MaxSize")
  }

  def get(id: String): ClientInstanceState = _cache.get(id)

  //
  // Invalidates the client metrics cache by iterating through all the client instances and do one of the following:
  //     1. TTL operation -- Cleans up all the cache entries that are expired beyond TTL allowed time.
  //     2. Adding a new group -- Update the metrics by appending the new metrics from the new group
  //     3. Deleting an existing group -- Update the metrics by deleting the metrics from the deleted group.
  //     4. Updating an existing group. -- delete the old metrics and add the new metrics.
  //
  def invalidate(oldGroup: SubscriptionGroup, newGroup: SubscriptionGroup, operation: ClientMetricsCacheOperation) = {
    operation match {
      case CM_SUBSCRIPTION_TTL => cleanupTtlEntries()
      case CM_SUBSCRIPTION_ADDED => addCMSubscriptionGroup(newGroup)
      case CM_SUBSCRIPTION_DELETED => deleteCmSubscriptionGroup(newGroup)
      case CM_SUBSCRIPTION_UPDATED => updateCmSubscriptionGroup(oldGroup, newGroup)
      case _ => throw new InvalidRequestException("Invalid request, can not update the client metrics cache")
    }
  }


  // Client instance specific state is maintained in broker memory up to MAX(60*1000, PushIntervalMs * 3) milliseconds
  // There is no persistence of client instance metrics state across broker restarts or between brokers.
  private def cleanupTtlEntries() = {
    val expiredElements = new ListBuffer[ClientInstanceState] ()
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

  // Updates the client instance objects
  private def addCMSubscriptionGroup(cmSubscriptionGroup: SubscriptionGroup) = {
    val affectedElements = new ListBuffer[ClientInstanceState] ()
    _cache.values().forEach(v =>
        if (v.getClientSelector.isMatched(cmSubscriptionGroup.getClientMatchingPatterns)) {
          v.getSubscriptionGroups.add(cmSubscriptionGroup)
          affectedElements.append(ClientInstanceState(v, v.getSubscriptionGroups))
        }
    )
    affectedElements.foreach(x => _cache.replace(x.getId.toString, x))
  }

  private def deleteCmSubscriptionGroup(cmSubscriptionGroup: SubscriptionGroup) = {
    val affectedElements = new ListBuffer[ClientInstanceState] ()
    _cache.values().forEach(v =>
      if (v.getClientSelector.isMatched(cmSubscriptionGroup.getClientMatchingPatterns)) {
        v.getSubscriptionGroups.remove(cmSubscriptionGroup)
        if (!v.getSubscriptionGroups.isEmpty)
          affectedElements.append(ClientInstanceState(v, v.getSubscriptionGroups))
      }
    )
    affectedElements.foreach(x => _cache.replace(x.getId.toString, x))
  }

  private def updateCmSubscriptionGroup(oldGroup: SubscriptionGroup, newGroup: SubscriptionGroup) = {
    val affectedElements = new ListBuffer[ClientInstanceState] ()
    _cache.values().forEach(v => {
      var changed: Boolean = false
      if (v.getClientSelector.isMatched(oldGroup.getClientMatchingPatterns)) {
        changed = true
        v.getSubscriptionGroups.remove(oldGroup)
      }
      if (v.getClientSelector.isMatched(newGroup.getClientMatchingPatterns)) {
        changed = true
        v.getSubscriptionGroups.add(newGroup)
      }
      if (!v.getSubscriptionGroups.isEmpty && changed) {
          affectedElements.append(ClientInstanceState(v, v.getSubscriptionGroups))
      }
    })

    affectedElements.foreach(x => _cache.replace(x.getId.toString, x))
  }

  private def isExpired(element: ClientInstanceState) = {
    val delta = element.getCurrentTime.getTime - element.getLastMetricsReceivedTs.getTime
    delta > Math.max(3 * element.getPushIntervalMs, DEFAULT_TTL_MS)
  }

  def invalidate(reason: String): Future[Int] = Future {
    val preCleanupSize = _cache.size
    invalidate(null, null, CM_SUBSCRIPTION_TTL)
    _cache.size - preCleanupSize
  }

  // Launches the asynchronous task to clean the client metric subscriptions that are expired in the cache.
  def runCmCacheCleaner(reason: String): Unit = {
    gcTs.synchronized {
      val currentTime = Calendar.getInstance.getTime
      if (currentTime.getTime - gcTs.getTime > CM_CACHE_GC_INTERVAL) {
        gcTs = currentTime
        invalidate(reason).onComplete {
          case Success(value) => info(s"Client Metrics subscriptions cache cleaned up $value entries")
          case Failure(exception) => info(s"Client Metrics subscription ache cleanup failed ${exception.getMessage}")
        }
      }
    }
  }

}
