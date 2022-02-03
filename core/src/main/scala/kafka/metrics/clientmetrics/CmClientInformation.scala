package kafka.metrics.clientmetrics

import kafka.Kafka.info
import kafka.metrics.clientmetrics.CmClientInformation._
import kafka.network.RequestChannel
import org.apache.kafka.common.errors.InvalidConfigurationException

import scala.collection.mutable


/**
 * Information from the client's metadata is gathered from the client's request.
 */
object CmClientInformation {
  val CLIENT_ID = "client_id"
  val CLIENT_INSTANCE_ID = "client_instance_id"
  val CLIENT_SOFTWARE_NAME = "client_software_name"
  val CLIENT_SOFTWARE_VERSION = "client_software_version"
  val CLIENT_SOURCE_ADDRESS = "client_source_address"
  val CLIENT_SOURCE_PORT = "client_source_port"

  val matchersList = List(CLIENT_ID, CLIENT_INSTANCE_ID, CLIENT_SOFTWARE_NAME,
                          CLIENT_SOFTWARE_VERSION, CLIENT_SOURCE_ADDRESS, CLIENT_SOURCE_PORT)

  def getMatchers = matchersList

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

  /**
   * Parses the client matching patterns and builds a map with entries that has
   * (PatternName, PatternValue) as the entries.
   *  Ex: "VERSION=1.2.3" would be converted to a map entry of (Version, 1.2.3)
   * @param patterns List of client matching pattern strings
   * @return
   */
  def parseMatchingPatterns(patterns: List[String]) : Map[String, String] = {
    val patternsMap = mutable.Map[String, String]()
    patterns.foreach(x =>  {
      val nameValuePair = x.split("=").map(x => x.trim)
      if (nameValuePair.size != 2 || !matchersList.contains(nameValuePair(0))) {
        throw new InvalidConfigurationException("Illegal client matching pattern: " + x)
      }
      patternsMap += (nameValuePair(0) -> nameValuePair(1))
    })
    patternsMap.toMap
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
