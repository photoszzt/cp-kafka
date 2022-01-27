package kafka.server

import kafka.metrics.clientmetrics.{ClientInstanceSelector, ClientInstanceState, ClientMetricsCache, ClientMetricsConfig}
import kafka.network.RequestChannel
import kafka.server.ClientMetricsManager.getSupportedCompressionTypes
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.requests.{GetTelemetrySubscriptionRequest, GetTelemetrySubscriptionResponse}

import java.util.{Calendar, Properties}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._


object ClientMetricsManager {
  private val _instance =  new ClientMetricsManager
  def get = _instance

  def getSupportedCompressionTypes: List[java.lang.Byte] = {
    val compressionTypes = new ListBuffer[java.lang.Byte]
    CompressionType.values.foreach(x => compressionTypes.append(x.id.toByte))
    compressionTypes.toList
  }

}

class ClientMetricsManager {
  val clientInstanceCache = new ClientMetricsCache
  var cmCacheGCTs = Calendar.getInstance.getTime

  def processGetSubscriptionRequest(request: RequestChannel.Request,
                                    config: KafkaConfig, throttleMs: Int): GetTelemetrySubscriptionResponse  = {
    val subscriptionRequest = request.body[GetTelemetrySubscriptionRequest]
    var clientInstanceId = subscriptionRequest.getClientInstanceId
    if (clientInstanceId == null || clientInstanceId == Uuid.ZERO_UUID) {
      clientInstanceId = Uuid.randomUuid()
    }
    var instanceState = clientInstanceCache.get(clientInstanceId.toString)
    if (instanceState == null) {
      instanceState = ClientInstanceState(clientInstanceId,
                                          ClientInstanceSelector(request, clientInstanceId.toString),
                                          ClientMetricsConfig.getClientSubscriptionGroups)
      clientInstanceCache.put(instanceState)
    }

    val data =  new GetTelemetrySubscriptionsResponseData()
        .setThrottleTimeMs(throttleMs)
        .setClientInstanceId(clientInstanceId)
        .setSubscriptionId(instanceState.getSubscriptionId.toInt) // ?? TODO: should we use LONG instead of down casting into int?
        .setAcceptedCompressionTypes(getSupportedCompressionTypes.asJava)
        .setPushIntervalMs(instanceState.getPushIntervalMs)
        .setDeltaTemporality(config.clientMetricsDeltaTemporality)
        .setRequestedMetrics(instanceState.metrics.asJava)

    new GetTelemetrySubscriptionResponse(data)
  }

  def updateSubscription(groupId :String, properties :Properties) = {
    ClientMetricsConfig.updateClientSubscription(groupId, properties, clientInstanceCache)
  }
}

