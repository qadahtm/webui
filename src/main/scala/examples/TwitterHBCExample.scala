
package examples

import java.util.concurrent.LinkedBlockingQueue
import com.twitter.hbc.core._
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.endpoint.Location
import com.twitter.hbc.core.endpoint.Location.Coordinate
import com.twitter.hbc.httpclient.auth.OAuth1
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import java.util.ArrayList
import spray.json.JsonParser
import spray.json.JsArray
import spray.json.JsString

object TwitterHBCExample {
  
  def main(args:Array[String]) = {
    run(args(0),args(1),args(2),args(3))
  }
  
  def run(consumerKey:String, consumerSecret:String, token:String, secret:String) : Unit = {
    val bqueue = new LinkedBlockingQueue[String](10000)
    val geotagged = new LinkedBlockingQueue[String](10000)
    val endpoint = new StatusesFilterEndpoint()
    // New York -74,40,-73,41
    // USA: w:-125.51, n: 49.44, e:-66.45. s:23.81
    
    val ny_sw = new Coordinate(-74D,40D)
    val ny_ne = new Coordinate(-73D,41D)
    val ny_loc = new Location(ny_sw,ny_ne)
    
    val usa_sw = new Coordinate(-125.51D,23.81D)
    val usa_ne = new Coordinate(-66.45D,49.44D)
    val usa_loc = new Location(usa_sw,usa_ne)
    
    val locs = new ArrayList[Location]();
    
//    locs.add(ny_loc)
    locs.add(usa_loc)
    endpoint.locations(locs)
    
//    val terms = new ArrayList[String]();
    
//    terms.add("Purdue");
//    endpoint.trackTerms(terms)
    
    val auth:Authentication = new OAuth1(consumerKey, consumerSecret, token, secret);
    val client  = new ClientBuilder()
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(bqueue))
            .build();

    // Establish a connection
    println("Connecting to twitter ...")
    client.connect();
    println("Connected.")
    
    
    // discard tweets if buffer max is reached
    val bufferMax = 4000
    val bufferSize = 1000
    while (true){
      var tdc = 0
      var discard = false;
      
      var i = 0
      var ctweet = bqueue.poll()
      while (ctweet != null && i < bufferSize){
        val jo = JsonParser(ctweet).asJsObject
        val coos = jo.fields.get("coordinates")
        coos match {
          case Some(t) => {
            if (t != null){
//              println(ctweet) 
            
              geotagged.add(ctweet)
            }
          }
          case _ => {}
        }
        
        i = i + 1
        ctweet = bqueue.poll()
        
      }
      
      while (bqueue.size() > bufferMax){
        bqueue.take()
        tdc = tdc + 1
        discard = true
      }
      if (discard) {
        println(s"discarded ${tdc} tweets")
        tdc = 0
        discard = false;
      }
      
      println("Sleeping for 10 secs")
      Thread.sleep(10000) // sleep for 10 secs
      println(s"current bqueue buff size = ${bqueue.size()}")
      println(s"current geo buff size = ${geotagged.size()}")
    }
    
    
    // Do whatever needs to be done with messages
//    var msgRead = 0
//    Thread.sleep(3000)
//    println(s"current buff size = ${bqueue.size()}")
//    println("reading 10 messages")
//    for (msgRead <- 0 to 10) {
//      val msg = bqueue.take();
////      println(msg);
//    }
//    Thread.sleep(6000)
//    println(s"current buff size = ${bqueue.size()}")
//    var tdc = 0
//    while (bqueue.size() > 10){
//      bqueue.take()
//      tdc = tdc + 1
//    }
//    println(s"discarded ${tdc} tweets")
//    println(s"current buff size = ${bqueue.size()}")
//    println("reading another 10 messages")
//    for (msgRead <- 0 to 10) {
//      val msg = bqueue.take();
////      println(msg);
//    }
//    println(s"current buff size = ${bqueue.size()}")
//    Thread.sleep(2000)
//    println(s"current buff size = ${bqueue.size()}")
//    
//    println("stoping client now ...")
    //client.stop();
    
  }

}