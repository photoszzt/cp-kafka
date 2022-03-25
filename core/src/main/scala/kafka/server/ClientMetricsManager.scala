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

package kafka.server

import kafka.Kafka.info
import kafka.metrics.clientmetrics.{ClientMetricsCache, ClientMetricsConfig, ClientMetricsReceiverPlugin, CmClientInformation, CmClientInstanceState}
import kafka.network.RequestChannel
import kafka.server.ClientMetricsManager.{getCurrentTime, getSupportedCompressionTypes}
import org.apache.kafka.common.errors.{ClientMetricsException, ClientMetricsReceiverPluginNotFoundException}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.annotation.InterfaceStability.Evolving
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.requests.{GetTelemetrySubscriptionRequest, GetTelemetrySubscriptionResponse, PushTelemetryRequest, PushTelemetryResponse, RequestContext}

import java.util.{Calendar, Properties}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object ClientMetricsManager {
  private val _instance = new ClientMetricsManager
  def getInstance = _instance
  def getCurrentTime = Calendar.getInstance.getTime.getTime

  def getSupportedCompressionTypes: List[java.lang.Byte] = {
    val compressionTypes = new ListBuffer[java.lang.Byte]
    CompressionType.values.filter(x => x != CompressionType.NONE).foreach(x => compressionTypes.append(x.id.toByte))
    compressionTypes.toList
  }

  // TODO: Needs to integrate with external plugin changes..
  // if plugin is not configured, getMetricsSubscriptions and pushMetricSubscription needs to return errors
  // to the client.
  @Evolving
  def checkCmReceiverPluginConfigured()  = {
    if (ClientMetricsReceiverPlugin.getCmReceiver() == null) {
      throw new ClientMetricsReceiverPluginNotFoundException("Broker does not have any configured client metrics receiver plugin")
    }
  }

  def processGetTelemetrySubscriptionRequest(request: RequestChannel.Request,
                                             throttleMs: Int): GetTelemetrySubscriptionResponse = {
    val subscriptionRequest = request.body[GetTelemetrySubscriptionRequest]
    val clientInfo = CmClientInformation(request, subscriptionRequest.getClientInstanceId.toString)
    _instance.processGetSubscriptionRequest(subscriptionRequest, clientInfo, throttleMs)
  }

  def processPushTelemetryRequest(request: RequestChannel.Request, throttleMs: Int): PushTelemetryResponse = {
    val pushTelemetryRequest = request.body[PushTelemetryRequest]
    val clientInfo = CmClientInformation(request, pushTelemetryRequest.getClientInstanceId.toString)
    _instance.processPushTelemetryRequest(pushTelemetryRequest, request.context, clientInfo, throttleMs)
  }
}

class ClientMetricsManager {

  def getClientInstance(id: Uuid) =  {
    if (id != null && id != Uuid.ZERO_UUID)
      ClientMetricsCache.getInstance.get(id)
    else null
  }

  def processGetSubscriptionRequest(subscriptionRequest: GetTelemetrySubscriptionRequest,
                                    clientInfo: CmClientInformation,
                                    throttleMs: Int): GetTelemetrySubscriptionResponse = {
    var clientInstanceId = subscriptionRequest.getClientInstanceId
    if (clientInstanceId == null || clientInstanceId == Uuid.ZERO_UUID) {
      clientInstanceId = Uuid.randomUuid()
    }
    var clientInstance = getClientInstance(clientInstanceId)
    if (clientInstance == null) {
      clientInstance = createClientInstance(clientInstanceId, clientInfo)
    }

    val data =  new GetTelemetrySubscriptionsResponseData()
        .setThrottleTimeMs(throttleMs)
        .setClientInstanceId(clientInstanceId)
        .setSubscriptionId(clientInstance.getSubscriptionId) // TODO: should we use LONG instead of int?
        .setAcceptedCompressionTypes(getSupportedCompressionTypes.asJava)
        .setPushIntervalMs(clientInstance.getPushIntervalMs)
        .setDeltaTemporality(true)
        .setErrorCode(Errors.NONE.code())
        .setRequestedMetrics(clientInstance.getMetrics.asJava)

    if (clientInstance.isDisabledForMetricsCollection) {
      info(s"Metrics collection is disabled for the client: ${clientInstance.getId.toString}")
    }

    clientInstance.updateLastAccessTs(getCurrentTime)

    new GetTelemetrySubscriptionResponse(data)
  }

  def validatePushRequest(pushTelemetryRequest: PushTelemetryRequest,
                          clientInfo: CmClientInformation): Unit = {

    def isSupportedCompressionType(id: Int) : Boolean = {
      var supported = true
      try {
        CompressionType.forId(id)
      } catch {
        case e: IllegalArgumentException => supported = false
      }
      supported
    }

    val clientInstanceId = pushTelemetryRequest.getClientInstanceId
    if (clientInstanceId == null || clientInstanceId == Uuid.ZERO_UUID) {
      val msg = String.format("Invalid request from the client [%s], missing client instance id", clientInfo.getClientId)
      throw new ClientMetricsException(msg, Errors.INVALID_REQUEST)
    }

    var clientInstance = getClientInstance(clientInstanceId)
    if (clientInstance == null) {
      clientInstance = createClientInstance(clientInstanceId, clientInfo)
    }

    if (pushTelemetryRequest.getSubscriptionId != clientInstance.getSubscriptionId) {
      val msg = String.format("Client's subscription id [%d] != Broker's cached client's subscription id [%d]",
        pushTelemetryRequest.getSubscriptionId, clientInstance.getSubscriptionId)
      throw new ClientMetricsException(msg, Errors.UNKNOWN_CLIENT_METRICS_SUBSCRIPTION_ID)
    }

    if (!isSupportedCompressionType(pushTelemetryRequest.data().compressionType)) {
      val msg = String.format("Unknown compression type [%d] is received in PushTelemetryRequest from %s",
        pushTelemetryRequest.data().compressionType(), pushTelemetryRequest.getClientInstanceId.toString)
      throw new ClientMetricsException(msg, Errors.UNSUPPORTED_COMPRESSION_TYPE)
    }

    if (!clientInstance.canAcceptPushRequest(pushTelemetryRequest.isClientTerminating)) {
      val msg = String.format("Request from the client [%s] arrived before the throttling time",
        pushTelemetryRequest.getClientInstanceId.toString)
      throw new ClientMetricsException(msg, Errors.THROTTLING_QUOTA_EXCEEDED)
    }
  }

  def processPushTelemetryRequest(pushTelemetryRequest: PushTelemetryRequest,
                                  requestContext: RequestContext,
                                  clientInfo: CmClientInformation,
                                  throttleMs: Int): PushTelemetryResponse = {

    def createResponse(errors: Errors) : PushTelemetryResponse = {
      var adjustedThrottleMs = throttleMs
      val clientInstance = getClientInstance(pushTelemetryRequest.getClientInstanceId)

      // Before sending the response make sure to update the book keeping markers like
      // lastAccessTime, isTerminating flag etc..
      if (clientInstance != null) {
        adjustedThrottleMs = Math.max(clientInstance.getAdjustedPushInterval(), throttleMs)
        clientInstance.updateLastAccessTs(getCurrentTime)
        clientInstance.setTerminatingFlag(pushTelemetryRequest.isClientTerminating)
      }

      pushTelemetryRequest.createResponse(adjustedThrottleMs, errors)
    }

    var errorCode = Errors.NONE
    try {
      // Validate the push request parameters
      validatePushRequest(pushTelemetryRequest, clientInfo)

      // Push the metrics to the external client receiver plugin.
      val metrics = pushTelemetryRequest.data().metrics()
      if (metrics != null && !metrics.isEmpty) {
        val payload = ClientMetricsReceiverPlugin.createPayload(pushTelemetryRequest)
        ClientMetricsReceiverPlugin.getCmReceiver().exportMetrics(requestContext, payload)
      }
    } catch {
      case e: ClientMetricsException => {
        errorCode = e.getErrorCode
      }
    }

    // Finally, send the response back to the client
    createResponse(errorCode)
  }

  def updateSubscription(groupId :String, properties :Properties) = {
    ClientMetricsConfig.updateClientSubscription(groupId, properties)
  }

  def createClientInstance(clientInstanceId: Uuid, clientInfo: CmClientInformation): CmClientInstanceState = {
    val clientInstance = CmClientInstanceState(clientInstanceId, clientInfo,
                                               ClientMetricsConfig.getClientSubscriptions)
    // Add to the cache and if cache size > max entries then time to make some room by running
    // GC to clean up all the expired entries in the cache.
    ClientMetricsCache.getInstance.add(clientInstance)
    ClientMetricsCache.runGCIfNeeded()
    clientInstance
  }
}

