/*
   Copyright 2015 - Thamir Qadah
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package cruncher.ui

import spray.routing.SimpleRoutingApp
import akka.actor.ActorSystem
import spray.http.HttpHeaders._
import spray.http.ContentTypes._
import spray.http.HttpResponse
import spray.http.HttpEntity
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import spray.json.JsObject
import spray.http.ChunkedResponseStart
import spray.json.JsString
import spray.http.MessageChunk
import akka.actor.PoisonPill
import spray.json.JsNumber
import spray.http.ChunkedMessageEnd
import akka.actor.Props
import scala.concurrent.duration._
import spray.http.HttpData
import spray.http.MediaType
import spray.http.MediaTypes
import spray.http.MediaTypes._
import akka.io.Tcp
import spray.can.Http
import spray.http.HttpCharsets
import kafka.serializer._
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat
import spray.json.JsValue
import spray.json.DeserializationException
import utils.BerlinMODLatLngConverter
import utils.KafkaStringMessage
import utils.Helper
import com.typesafe.config.ConfigFactory
import java.io.File
import kafka.producer.KeyedMessage
import utils.PubSubActor
import utils.Forward
import utils.Subscribe
import utils.Unsubscribe
import spray.json.JsArray
import akka.actor.actorRef2Scala
import spray.http.ContentType.apply
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.directives.ParamDefMagnet.apply
import utils.AppJsonProtocol._
import utils._
import spray.routing.authentication.BasicAuth
import spray.routing.authentication.UserPass
import scala.concurrent.Future
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.collection.JavaConversions._
import spray.json.JsonParser
import com.turn.platform.cheetah.partitioning.horizontal._
import scala.io.Source
import ui._
import spray.http.StatusCodes
import org.apache.kafka.clients.producer.ProducerRecord
import spray.json.JsBoolean
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout

object CruncherWebserver extends App with SimpleRoutingApp {

  object TornadoWebserverJsonFormat extends DefaultJsonProtocol

  val conf = Helper.getConfig()

  val asystem = ActorSystem(conf.getString("webserver.actorSystem.name"),
    Helper.getActorSystemConfig(conf.getString("webserver.hostname"),
      conf.getString("webserver.actorSystem.port")))

  implicit val system = ActorSystem("webserver")
  implicit val ec = system.dispatcher
  implicit val log = system.log

  val host = conf.getString("webserver.hostname")
  val port = conf.getInt("webserver.port")

  val queryPubSubActor = asystem.actorOf(Props(classOf[PubSubActor[String]]), conf.getString("webserver.queryPubsubActorName"))
  log.info("created query pubsub service at : " + queryPubSubActor.path)

  val resultPubSubActor = asystem.actorOf(Props(classOf[PubSubActor[String]]), conf.getString("webserver.resultPubsubActorName"))
  log.info("created result pubsub service at : " + resultPubSubActor.path)
  
  
  val sparkAppMediator = asystem.actorOf(Props(classOf[NetworkSocketDataBufferServer], "localhost", 2016,resultPubSubActor), "sparkappmed")
  
  val EventStreamType = register(
    MediaType.custom(
      mainType = "text",
      subType = "event-stream",
      compressible = true,
      binary = false))

      
  def createStatusResponse(status:String, msg:String) = {
    JsObject("status" -> JsString(status) , "msg" -> JsString(msg) ).toString()
  }    
 

  val getAvailableDataFiles = path("datafiles") {
    get {
      val datafiles = new File(conf.getString("webserver.data.dir"))
      complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
        JsArray(datafiles.listFiles().filter {
          f => !f.isDirectory && """.*\.csv""".r.findFirstIn(f.getName()).isDefined
        }.map(f => JsString(f.getName()))
          .toVector).toString)))
    }
  }

  //  val tornado_ds_map = new  

  val cruncher_datasources = pathPrefix("cruncher") {
    path("datasources") {
      get {
        complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
          JsArray(JsObject("sourceName" -> JsString("Tweets")),
            JsObject("sourceName" -> JsString("POI_Data")),
            JsObject("sourceName" -> JsString("MovingObjects"))).toString)))
      }
    }
  }

  val registeredQueries = Map[String,JsObject]()
  
  val cruncher_output_cqueries = path("cruncher" / "output" / "cqueries" / Segment){ 
       qname =>{
         var resp: String = ""
         Catalog.json_cqueries.get(qname) match {
           case Some(q) => {
//             log.info("Got a request for results of : "+q.toString())
             complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
                q.toString())))
             
           } 
           case None => {
            complete(StatusCodes.NotFound)
           }
         }
         
       }}
  def findOperator(plan:JsObject, oname:String):JsObject = {
    if (plan.fields.get("type").get.asInstanceOf[JsString].value == oname) {
      plan
    }
    else {
      // return first (leftmost) child matching type name
      val des = plan.fields.get("children").get.asInstanceOf[JsArray].elements.map(x => findOperator(x.asJsObject,oname))
      des.head      
    }
  }
  
  val cruncher_queries = path("cruncher" / "queries") {
      get {
        complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
          Catalog.getRegisteredCQueries.toString())))
      }~
      post{
        ctx => {       
          var ret:String = ""
          val jstr = ctx.request.entity.asString
          val jo = JsonParser(jstr)
//          log.info("received request, entity = " + jo.prettyPrint)
          log.info(jo.prettyPrint)
          
          val selectOp = findOperator(jo.asJsObject.fields.get("plan").get.asJsObject,"select")
//          log.info(rangePred.prettyPrint) 
        
          val qname = jo.asJsObject.fields.get("name").get.asInstanceOf[JsString].value
          
            jo.asJsObject.fields.get("type") match {
            case Some(JsString(cq)) => {
              cq match {
                case "cquery" => {
                  // continuous query
                  
                  Catalog.json_cqueries.get(qname) match {
                    case Some(qn) =>{
                      ret = createStatusResponse("error","query is already registered : "+qname)
                    }
                    case None => {
                      // register query
                      Catalog.json_cqueries += (qname -> jo.asJsObject)
                      
                      // publish to Cruncher if needed
                      CruncherDataBuffer.addQuery(jstr)
                      
                      ret = createStatusResponse("success","query registered : "+qname)
                    }
                  }
                  
                  
                  
                }
                case "squery" => {
                  // snapshot query and static queries
                  ret = createStatusResponse("error","not handling snapshot/static queris for now")
                }
                case _ => {
                  ret = createStatusResponse("error","unknown query type : "+cq)
                }
              }
            }
          }
          
          ctx.complete(ret)
        }
      }~
      delete {
        ctx => {
          val jstr = ctx.request.entity.asString
          val jo = JsonParser(jstr)
          log.info(jo.prettyPrint)
          var resp = ""
          val qname = jo.asJsObject.fields.get("name") 
          qname match {
            case Some(x) => {
              val key = x.asInstanceOf[JsString].value
              
              Catalog.json_cqueries.remove(key) match {
                case Some(o) => {
                  resp = createStatusResponse("success", "successfully removed query : "+key)
                }
                case None => {
                  resp = createStatusResponse("error", "cannot remove nonexistent query : "+key)
                }
              }
            }
            case None => {
              resp = createStatusResponse("error", "malformed/invalid delete request ")
            }
          }
          
          ctx.complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`, resp)))
        }
      }
  }
  
  val systemConfig = path("cruncher" / "config") {
    val kafkaConf = JsObject("enabled" -> JsBoolean(false))
    val resp = JsObject("kafka" -> kafkaConf)
    complete(HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
          resp.toString())))
  }

  startServer(interface = host, port = port) {
    getFromDirectory("ui/public") ~
      cruncher_queries ~
      cruncher_output_cqueries~
      systemConfig~
      cruncher_datasources~
      path("mock-output-stream") {
        ctx =>
          {
            log.info("Mocking an output stream")
            val aprops = Props(classOf[RandomPointWithTextStreamer], ctx.responder, EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      } ~
      path("output-stream") {
        ctx =>
          {
            log.info("Creating a subscription to " + resultPubSubActor)
            val aprops = Props(classOf[BufferingStreamer], ctx.responder, resultPubSubActor, (Helper.selfString _), EventStreamType)
            val streamer = system.actorOf(aprops)
          }
      } ~
      getAvailableDataFiles
  }
}