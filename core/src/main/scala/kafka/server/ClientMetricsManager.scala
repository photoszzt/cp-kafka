package kafka.server

import kafka.metrics.clientmetrics.{CmClientInformation, CmClientInstanceState, ClientMetricsCache, ClientMetricsConfig}
import kafka.network.RequestChannel
import kafka.server.ClientMetricsManager.{CM_CACHE_MAX_SIZE, getSupportedCompressionTypes}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.requests.{GetTelemetrySubscriptionRequest, GetTelemetrySubscriptionResponse}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object ClientMetricsManager {
  val CM_CACHE_GC_INTERVAL = 5 * 60 * 1000 // 5 minutes
  val CM_CACHE_MAX_SIZE = 1024

  private val _instance = new ClientMetricsManager
  def get = _instance

  def getSupportedCompressionTypes: List[java.lang.Byte] = {
    val compressionTypes = new ListBuffer[java.lang.Byte]
    CompressionType.values.foreach(x => compressionTypes.append(x.id.toByte))
    compressionTypes.toList
  }

}

class ClientMetricsManager {
  val clientInstanceCache = ClientMetricsCache.getInstance
  def getCacheSize = clientInstanceCache.getSize
  def clearCache() = clientInstanceCache.clear()
  def getClientInstance(id: Uuid) = clientInstanceCache.get(id)

  def processGetSubscriptionRequest(request: RequestChannel.Request,
                                    config: KafkaConfig, throttleMs: Int): GetTelemetrySubscriptionResponse = {
    val subscriptionRequest = request.body[GetTelemetrySubscriptionRequest]
    var clientInstanceId = subscriptionRequest.getClientInstanceId
    if (clientInstanceId == null || clientInstanceId == Uuid.ZERO_UUID) {
      clientInstanceId = Uuid.randomUuid()
    }
    var clientInstance = getClientInstance(clientInstanceId)
    if (clientInstance == null) {
      clientInstance = createClientInstance(clientInstanceId, CmClientInformation(request, clientInstanceId.toString))
    }

    val data =  new GetTelemetrySubscriptionsResponseData()
        .setThrottleTimeMs(throttleMs)
        .setClientInstanceId(clientInstanceId)
        .setSubscriptionId(clientInstance.getSubscriptionId.toInt) // ?? TODO: should we use LONG instead of down casting into int?
        .setAcceptedCompressionTypes(getSupportedCompressionTypes.asJava)
        .setPushIntervalMs(clientInstance.getPushIntervalMs)
        .setDeltaTemporality(config.clientMetricsDeltaTemporality)
        .setRequestedMetrics(clientInstance.metrics.asJava)

    new GetTelemetrySubscriptionResponse(data)
  }

  def updateSubscription(groupId :String, properties :Properties) = {
    ClientMetricsConfig.updateClientSubscription(groupId, properties, clientInstanceCache)
  }

  def createClientInstance(clientInstanceId: Uuid, clientInfo: CmClientInformation): CmClientInstanceState = {
    val clientInstance = CmClientInstanceState(clientInstanceId, clientInfo, ClientMetricsConfig.getClientSubscriptionGroups)
    // add to the cache and if cache size > max entries then time to make some room.
    clientInstanceCache.add(clientInstance)
    if (clientInstanceCache.getSize >  CM_CACHE_MAX_SIZE) {
      ClientMetricsCache.runGC()
    }
    clientInstance
  }

}

