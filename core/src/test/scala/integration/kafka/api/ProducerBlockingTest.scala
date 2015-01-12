
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

package kafka.api.test

import org.junit.Test
import org.junit.Assert._

import java.util.concurrent.{TimeUnit, ExecutionException}

import kafka.server.KafkaConfig
import kafka.integration.KafkaServerTestHarness
import kafka.utils.{TestZKUtils, TestUtils}

import org.apache.kafka.clients.producer._

import System.{currentTimeMillis => _time}

class ProducerBlockingTest extends KafkaServerTestHarness {
  def profile[R](code: => R, t: Long = _time) = (code, _time - t)

  val configs =
    for(props <- TestUtils.createBrokerConfigs(numConfigs = 1, enableControlledShutdown = false))
    yield new KafkaConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
      override val autoCreateTopicsEnable = false
    }

  var producer: KafkaProducer[Array[Byte],Array[Byte]] = null
  var duration: Long = 0

  val topic = "topic-blockingtest"

  val expectedBlockPeriod = 5000
  val fudge = 500
  val fastResponse = 30

  def record(t: String = topic) = new ProducerRecord[Array[Byte],Array[Byte]](t, null, "key".getBytes, new Array[Byte](100))

  override def tearDown() {
    if (producer != null) producer.close()

    super.tearDown()
  }

  def assertDuration(operation: String, duration: Long, expected: Long) {
    assertTrue(s"$operation - Time duration expected: $expected, actual: $duration", duration < expected + fudge  &&  duration > expected - fudge)
  }

  def assertFast(operation: String, duration: Long) {
    assertTrue(s"$operation - Expected time duration for to be fast (< $fastResponse). Actual: $duration", duration < fastResponse)
  }

  /**
   * This test shows the default blocking behavior that can occur. This default behavior is questionable for an API that returns a future as it is not guaranteed to not block
   */
  @Test
  def testBlockingWhenSendAndServerDown() {
    servers.map(_.shutdown())

    producer = TestUtils.createNewProducer(brokerList, acks = 0, blockOnBufferFull = false, metadataFetchTimeout = expectedBlockPeriod)
    assertTrue("Always indicate things are initialized unless pre-init is specified", producer.isInitialized)

    val (_, duration) = profile {
      intercept[ExecutionException] {
        producer.send(record()).get
      }
    }

    assertDuration("Default send blocking", duration, expectedBlockPeriod)
  }

  /**
   * If auto create == false and topic doesn't exist things should fast fail. Instead, the API blocks for metadataFetchTimeout
   * Should open a different ticket for this.
   */
  @Test
  def testBlockingOnMetaOnMissingTopic() {
    producer = TestUtils.createNewProducer(
      brokerList, acks = 0,
      blockOnBufferFull = false,
      metadataFetchTimeout = expectedBlockPeriod)
    assertTrue("Always indicate things are initialized unless pre-init is specified", producer.isInitialized)

    // Since auto create is false, let's see how long it takes for send to fail
    val (_, duration) = profile {
      intercept[ExecutionException] {
        producer.send(record()).get(10000, TimeUnit.MILLISECONDS)
      }
    }

    assertDuration("Blocking when up, but topic not created", duration, expectedBlockPeriod)
  }

  /**
   * When preInitFailureAction is fail and kafka server is down expect producer creation to fail within preInitTimeout
   */
  @Test
  def testFailInitWhenPreInitSpecified() {
    TestUtils.createTopic(zkClient, topic, 1, 1, servers)

    servers.map(_.shutdown())

    val (_, duration) = profile {
      producer = TestUtils.createNewProducer(
        brokerList, acks = 0,
        blockOnBufferFull = false,
        preInitTimeout = Some(expectedBlockPeriod),
        preInitTopics = Some(topic))
    }

    assertFalse("Producer should indicate it is not initialized", producer.isInitialized)
    assertDuration("Blocking during producer creation failure", duration, expectedBlockPeriod)
  }


  /**
   * When preInitFailureAction is ignore and kafka server is down expect sends to fast fail and then sends to succeed once server is up
   */
  @Test
  def testPreInitIgnoreFailAndSend() {
    TestUtils.createTopic(zkClient, topic, 1, 1, servers)

    servers.map(_.shutdown())

    val (_, duration) = profile {
      producer = TestUtils.createNewProducer(
        brokerList, acks = 0,
        blockOnBufferFull = false,
        preInitTimeout = Some(expectedBlockPeriod),
        preInitTopics = Some(topic))
    }

    assertDuration("Blocking during producer init failure ignore", duration, expectedBlockPeriod)
    assertFalse("Producer is not yet initialized", producer.isInitialized)

    // This send should return very quickly
    val e = intercept[ExecutionException] {
      producer.send(record()).get(10, TimeUnit.MILLISECONDS)
    }
    assertEquals(e.getCause.getMessage, "Producer is not yet initialized")

    servers.map(_.startup())

    // Give producer time to correctly init
    while (!producer.isInitialized) {
      Thread.sleep(100)
    }

    val (_, sendDuration) = profile {
      producer.send(record())
    }
    assertFast("Valid send after server started", sendDuration)
  }

  @Test
  def testStillBlockIfAttemptingToSendToNewTopicAfterPreInit() {
    TestUtils.createTopic(zkClient, topic, 1, 1, servers)

    producer = TestUtils.createNewProducer(
      brokerList, acks = 0,
      blockOnBufferFull = false,
      metadataFetchTimeout = expectedBlockPeriod,   // This should be set low when using pre-init
      preInitTimeout = Some(1000),
      preInitTopics = Some(topic))

    assertTrue("Producer is initialized", producer.isInitialized)

    servers.map(_.shutdown())

    val (_, expectedFast) = profile {
      producer.send(record())
    }

    assertFast("Valid send after servers shutdown", expectedFast)

    val (_, blockTime) = profile {
      producer.send(record("nometadata"))
    }

    assertDuration("Blocking in initialized client for a topic with no meta data", blockTime, expectedBlockPeriod)

    // Start the server backup so that the sends that are queued can progress
    // Could alternatively just null out producer
    servers.map(_.startup())
  }
}
