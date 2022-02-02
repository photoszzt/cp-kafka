package kafka.metrics.clientmetrics

import kafka.Kafka.info
import kafka.metrics.clientmetrics.CmClientInformation._
import kafka.network.RequestChannel
import org.apache.kafka.common.errors.InvalidConfigurationException


/**
 * Information from the client's metadata is gathered from the client's request.
 */
object CmClientInformation {
  val CLIENT_INSTANCE_ID = "client_instance_id"
  val CLIENT_ID = "client_id"
  val CLIENT_SOFTWARE_NAME = "client_software_name"
  val CLIENT_SOFTWARE_VERSION = "client_software_version"
  val CLIENT_SOURCE_ADDRESS = "client_source_address"
  val CLIENT_SOURCE_PORT = "client_source_port"

  def apply(request: RequestChannel.Request, clientInstanceId: String): CmClientInformation = {
    val instance = new CmClientInformation
    instance.init(clientInstanceId, request.context.clientId(), request.context.clientInformation.softwareName(),
                  request.context.clientInformation.softwareVersion(), request.context.clientAddress.getHostAddress,
                  request.context.clientAddress.getHostAddress )
    instance
  }

  def apply(clientInstanceId: String, clientId: String, softwareName: String,
            softwareVersion: String, hostAddress: String, port: String): CmClientInformation = {
    val instance = new CmClientInformation
    instance.init(clientInstanceId, clientId, softwareName, softwareVersion, hostAddress, port )
    instance
  }

}

class CmClientInformation {
  var attributesMap = scala.collection.mutable.Map[String, String]()

  private def init(clientInstanceId: String,
                   clientId: String,
                   softwareName: String,
                   softwareVersion: String,
                   hostAddress: String,
                   port: String): Unit = {
    attributesMap(CLIENT_INSTANCE_ID) = clientInstanceId
    attributesMap(CLIENT_ID) = clientId
    attributesMap(CLIENT_SOFTWARE_NAME) = softwareName
    attributesMap(CLIENT_SOFTWARE_VERSION) = softwareVersion
    attributesMap(CLIENT_SOURCE_ADDRESS) = hostAddress
    attributesMap(CLIENT_SOURCE_PORT) = port // TODO: how to get the client's port info.
  }

  def isMatched(matchingPatterns: Map[String, String]) : Boolean = {
    try {
      if (matchingPatterns == null || matchingPatterns.isEmpty) {
        throw new InvalidConfigurationException("Empty client metrics matching patterns")
      }
      matchingPatterns.foreach {
      case (k, v) =>
          val attribute = attributesMap.getOrElse(k, null)
          if (attribute == null || !matchPattern(attribute, v)) {
            throw new InvalidConfigurationException(k)
          }
      }
      true
    } catch {
      case e: InvalidConfigurationException =>
        info(s"Unable to find the matching client subscription for the client ${e.getMessage}")
        false
    }
  }

  private def matchPattern(inputStr: String, pattern: String) : Boolean = {
    inputStr.matches(pattern)
  }

}
