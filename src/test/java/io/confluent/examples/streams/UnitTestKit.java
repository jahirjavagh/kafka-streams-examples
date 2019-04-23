/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility functions to make unit testing more convenient.
 */
public class UnitTestKit {

  public static <K, V> void writeRecordsToTopic(final TopologyTestDriver testDriver,
                                                final String topic,
                                                final List<KeyValue<K, V>> records,
                                                final Serializer<K> keySerializer,
                                                final Serializer<V> valueSerializer) {
    final ConsumerRecordFactory<K, V> f = new ConsumerRecordFactory<>(keySerializer, valueSerializer);
    testDriver.pipeInput(f.create(topic, records, Instant.ofEpochMilli(1).toEpochMilli(), Duration.ofMillis(100).toMillis()));
  }

  public static <K, V> Map<K, V> readTopicAsMap(final TopologyTestDriver testDriver,
                                                final String topic,
                                                final Deserializer<K> keyDeserializer,
                                                final Deserializer<V> valueDeserializer) {
    final Map<K, V> output = new HashMap<>();
    ProducerRecord<K, V> outputRow;
    while ((outputRow = testDriver.readOutput(topic, keyDeserializer, valueDeserializer)) != null) {
      output.put(outputRow.key(), outputRow.value());
    }
    return output;
  }

}