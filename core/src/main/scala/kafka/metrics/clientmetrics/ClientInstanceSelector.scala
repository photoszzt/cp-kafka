package kafka.metrics.clientmetrics

import kafka.Kafka.info
import kafka.metrics.clientmetrics.ClientInstanceSelector._
import kafka.network.RequestChannel
import org.apache.kafka.common.errors.InvalidConfigurationException


object ClientInstanceSelector {
  val CLIENT_INSTANCE_ID = "client_instance_id"
  val CLIENT_ID = "client_id"
  val CLIENT_SOFTWARE_NAME = "client_software_name"
  val CLIENT_SOFTWARE_VERSION = "client_software_version"
  val CLIENT_SOURCE_ADDRESS = "client_source_address"
  val CLIENT_SOURCE_PORT = "client_source_port"

  def apply(request: RequestChannel.Request, clientInstanceId: String): ClientInstanceSelector = {
    val instance = new ClientInstanceSelector
    instance.initClientInfo(request, clientInstanceId)
    instance
  }

}

class ClientInstanceSelector {
  var selectorsMap = scala.collection.mutable.Map[String, String]()

  private def initClientInfo(request: RequestChannel.Request, clientInstanceId: String): Unit = {
    selectorsMap(CLIENT_INSTANCE_ID) = clientInstanceId
    selectorsMap(CLIENT_ID) = request.context.clientId()
    selectorsMap(CLIENT_SOFTWARE_NAME) = request.context.clientInformation.softwareName()
    selectorsMap(CLIENT_SOFTWARE_VERSION) = request.context.clientInformation.softwareVersion()
    selectorsMap(CLIENT_SOURCE_ADDRESS) = request.context.clientAddress.getHostAddress
    selectorsMap(CLIENT_SOURCE_PORT) = request.context.clientAddress.getHostAddress  ///TODO : how to get the client's port info??
  }

  def isMatched(matchingPatterns: Map[String, String]) : Boolean = {
    try {
      matchingPatterns.foreach {
      case (k, v) =>
          val selector = selectorsMap.getOrElse(k, null)
          if (selector == null || !matchPattern(selector, v)) {
            throw new InvalidConfigurationException(selector.toString)
          }
      }
      true
    } catch {
      case e: InvalidConfigurationException =>
        info(s"Unable to find the matching client subscription for the client ${e.getMessage}")
        false
    }
  }

  private def matchPattern(selector: String, pattern: String) : Boolean = {
    selector.matches(pattern)
  }


}
