/**
 * Copyright 2015
 * Author : Thamir Qadah 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import kafka.javaapi.consumer.ConsumerConnector
import kafka.serializer.StringDecoder

object KafkaUtils {

  def createStringKafkaProducer(serverList: String): KafkaProducer[String, String] = {
    val props = new java.util.Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverList)
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none")
    //props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200)
    //props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String,String](props)
  }

  def createHLKafkaConsumer(zk:String, groupId:String) = {
    val conf = KafkaConsumerHelper.createConsumerConfig(zk, groupId)
    kafka.consumer.Consumer.createJavaConsumerConnector(conf);
  }
  
  def createStringKafkaStream(topic:String, consumer:ConsumerConnector) = {
    val topicCountMap = Map(topic -> 1.asInstanceOf[Integer]).asJava

    val consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder(), new StringDecoder())
    val streams = consumerMap.get(topic)
    // return the first stream since we are using one topic
    streams.get(0)
  }

  // New API but not working for some reason
  def createStringKafkaConsumer(serverList:String, groupId:String) = {
    val props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverList)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS, "1000")
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY,"range")
     new KafkaConsumer[String,String](props)
  }

}