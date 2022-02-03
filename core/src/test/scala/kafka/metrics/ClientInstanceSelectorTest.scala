package kafka.metrics

import kafka.metrics.clientmetrics.CmClientInformation
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.junit.jupiter.api.Assertions.{assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.{Test, Timeout}

@Timeout(120)
class ClientInstanceSelectorTest {

  def createClientInstanceSelector(): CmClientInformation = {
    val client_instance_id = Uuid.randomUuid().toString
    val clientId = "testclient1"
    val softwareName = "Confluent.Java"
    val softwareVersion = "89.2.0"
    val hostAddress = "1.2.3.4"
    val port = "9092"
    CmClientInformation(client_instance_id, clientId, softwareName, softwareVersion, hostAddress, port)
  }

  @Test
  def testClientMatchingPattern(): Unit = {
    val selector = createClientInstanceSelector()
    assertFalse(selector.isMatched(Map("a" -> "b")))
    assertFalse(selector.isMatched(Map("software" -> "Java")))
    assertTrue(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Confluent.Java")))
    assertTrue(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Confluent.*",
      CmClientInformation.CLIENT_SOFTWARE_VERSION -> "89.2.0")))
    assertTrue(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Confl.ent.*a",
      CmClientInformation.CLIENT_SOFTWARE_VERSION -> "^8.*0")))
    assertTrue(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Confluent.Java$",
      CmClientInformation.CLIENT_SOFTWARE_VERSION -> "8..2.*")))
    assertFalse(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Confluent.Java",
      CmClientInformation.CLIENT_SOFTWARE_VERSION -> "8.1.*")))
    assertFalse(selector.isMatched(
      Map("AAA" -> "BBB", CmClientInformation.CLIENT_SOFTWARE_VERSION -> "89.2.0")))
    assertFalse(selector.isMatched(
      Map(CmClientInformation.CLIENT_SOFTWARE_VERSION -> "89.2.0", "AAA" -> "fff")))
    assertFalse(selector.isMatched(Map.empty))
  }

  @Test
  def testMatchingPatternParser(): Unit = {

    CmClientInformation.parseMatchingPatterns(ClientMetricsTestUtils.defaultClientMatchPatters)

    val patterns = List("ABC=something")
    assertThrows(classOf[InvalidConfigurationException], () => CmClientInformation.parseMatchingPatterns(patterns))

    val patterns2 = ClientMetricsTestUtils.defaultClientMatchPatters ++ patterns
    assertThrows(classOf[InvalidConfigurationException], () => CmClientInformation.parseMatchingPatterns(patterns2))
  }

}
