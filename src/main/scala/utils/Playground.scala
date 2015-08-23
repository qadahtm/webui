package utils

import spray.json.JsObject
import spray.json.JsString
import spray.json.JsNumber
import kafka.serializer.StringDecoder
import scala.collection.JavaConverters.mapAsJavaMapConverter

object Playground {
  def main(args :Array[String]) : Unit = {
    
    // converts CSV to JSON
    
//    val source = scala.io.Source.fromFile("data/sample_tweets.csv")
//    source.getLines().foreach { x => {
//      val arr = x.split(",")
//      println(JsObject("point" -> JsObject("lat" -> JsNumber(arr(2)), "lng" -> JsNumber(arr(3))), "oid" -> JsString(arr(0)), "text" -> JsString(arr(5))).toString())
//      
//    } }
    
    ////////
    
    /// Kafka Consumer Testing
    val zk = "localhost:2181"
    val cgid = "g1"
    val topic = "output"
    
    val conf = KafkaConsumerHelper.createConsumerConfig(zk, cgid)
    val consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
    val topicCountMap = Map(topic -> 1.asInstanceOf[Integer]).asJava

    val consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder(), new StringDecoder())
    val streams = consumerMap.get(topic)
    
    while (true) {
      val iter = streams.get(0).iterator()
      
//      iter.foreach(x => {
//        println("got a message: "+x.message())
//      })
      
      if (iter.hasNext()) {
        val msg = iter.next.message()
        consumer.commitOffsets()
        println("got a message: "+msg)
      } 
      
      println("====================  after foreach")
      Thread.sleep(100)      
    }
    
    
    
  }
}