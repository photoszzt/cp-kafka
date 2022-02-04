/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    val softwareName = "Apache.Java"
    val softwareVersion = "89.2.0"
    val clientHostAddress = "1.2.3.4"
    val clientPort = "9092"
    CmClientInformation(client_instance_id, clientId, softwareName, softwareVersion, clientHostAddress, clientPort)
  }

  @Test
  def testClientMatchingPattern(): Unit = {
    val selector = createClientInstanceSelector()
    assertFalse(selector.isMatched(Map("a" -> "b")))
    assertFalse(selector.isMatched(Map("software" -> "Java")))
    assertTrue(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Apache.Java")))
    assertTrue(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Apache.*",
      CmClientInformation.CLIENT_SOFTWARE_VERSION -> "89.2.0")))
    assertTrue(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Apa.he.*a",
      CmClientInformation.CLIENT_SOFTWARE_VERSION -> "^8.*0")))
    assertTrue(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Apache.Java$",
      CmClientInformation.CLIENT_SOFTWARE_VERSION -> "8..2.*")))
    assertFalse(selector.isMatched(Map(CmClientInformation.CLIENT_SOFTWARE_NAME -> "Apache.Java",
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
