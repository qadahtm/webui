package semnets

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.util.ByteString
import scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scalax.collection.edge.LDiEdge
import scalax.collection.edge.Implicits._
import akka.actor.ActorSystem
import java.net.InetSocketAddress
import akka.actor.Props
import akka.actor.ActorRef
import scala.concurrent.duration._
import java.io.File
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import utils.Forward
import utils.Helper
import SemNetJsonProtocol._
import spray.json._

object SemanticNodes {

  def main(args: Array[String]): Unit = {
    //val gA = Graph(3~>1.2)            // Graph[AnyVal,DiEdge](3, 1.2, 3~>1.2)

    val conf = Helper.getConfig()

    implicit val system = ActorSystem(conf.getString("semnet.actorSystem.name"),
      Helper.getActorSystemConfig(conf.getString("semnet.actorSystem.hostname"),
        conf.getString("semnet.actorSystem.port")))
    //    implicit val system = ActorSystem("SemNetEngine")
    implicit val ec = system.dispatcher
    implicit val log = system.log

    val conffile = new File("application.conf")
    val appconf = ConfigFactory.parseFile(conffile)
    val streamSourceHost = appconf.getString("streamServer.hostname")
    val streamSourcePort = appconf.getInt("streamServer.port")

    val remote_socket = new InetSocketAddress(streamSourceHost, streamSourcePort)

    val snactor = args.size match {
      case 1 => {
        args(0) match {
          case "-agg" => {
            system.actorOf(Props(classOf[SemNetBuilder], true))
          }
        }
      }
      case _ => {
        system.actorOf(Props(classOf[SemNetBuilder], false))
      }
    }
    //    val snprinter = system.actorOf(Props(classOf[SemNetPrinter],snactor))

    val client = system.actorOf(Client.props(remote_socket, snactor))

    system.awaitTermination
    //    system.scheduler.schedule(0 seconds, 5 seconds)(snprinter ! "print")
  }

}

class SemNetPrinter(ds: ActorRef) extends Actor with ActorLogging {
  def receive = {
    case SemNetState(cs, rs) => {
      println("Concepts:" + cs)
      println("Relations:" + rs)
    }
    case "print" => {
      ds ! GetSemNetState
    }
    case "printJson" => {}
  }
}

class SemNetBuilder(val agg: Boolean) extends Actor with ActorLogging {

  val conffile = new File("application.conf")
  //log.info(conffile.getAbsolutePath())
  val appconf = ConfigFactory.parseFile(conffile)
  val webActorSystemName = appconf.getString("webserver.actorSystem.name")
  val webHost = appconf.getString("webserver.hostname")
  val webPort = appconf.getString("webserver.actorSystem.port")

  val resultPubSub = appconf.getString("webserver.resultPubsubActorName")
  implicit val default_timeout: Int = 100
  implicit val default_url: String = s"akka.tcp://$webActorSystemName@$webHost:$webPort/user/" + resultPubSub
  log.info("Will send updates to :" + default_url)
  //	val url = default_url
  //	val _timeout = default_timeout seconds
  //	val printer = Await.result(context.actorSelection(url).resolveOne(_timeout), _timeout)

  val relations = scala.collection.mutable.ArrayBuffer[Relation]()
  var trajectories = scala.collection.mutable.Map[Long, Array[TripSegTuple]]()

  val receivers = scala.collection.mutable.ArrayBuffer[ActorRef]()

  def receive = {

    case "RegisterMeAsReceiver" => {
      receivers += sender
      val concepts = trajectories.keys.map(x => Concept(x, x.toString))
      sender ! SemNetState(concepts, relations)
    }

    case GetSemNetState => {
      val concepts = trajectories.keys.map(x => Concept(x, x.toString))
      context.sender ! SemNetState(concepts, relations)
    }

    case data: ByteString => {
      val lines = data.decodeString("UTF-8")
      lines.split("\n")
        .foreach(line => {
          //        	println(line)
          // 7,74,2007-05-28-14:14:14.961,2007-05-28-14:14:16.961,21235.8,10200.1,21226.4,10174
          // tripId,startTime,endTime,lat,lng
          var skip = false
          val uline = line.split(",")
          val oid = uline(0).toLong

          val ot1 = uline(2)
          val ot2 = uline(3)

          //          val (lat1,lng1) = BerlinMODLatLngConverter.getLatLng(uline(4).toDouble, uline(5).toDouble)
          //          val (lat2,lng2) = BerlinMODLatLngConverter.getLatLng(uline(6).toDouble, uline(7).toDouble)

          val (lat1, lng1) = (uline(5).toDouble, uline(4).toDouble)
          val (lat2, lng2) = (uline(7).toDouble, uline(6).toDouble)
          if (lat1 == lat2 && lng1 == lng2) skip = true

          if (!skip) {
            trajectories.get(oid) match {
              case None => {
                // new trip
                val ttuple = TripSegTuple(oid, 1, LineSegment(LatLng(lat1, lng1), LatLng(lat2, lng2)), StringInterval(ot1, ot2))
                trajectories += (oid -> Array(ttuple))
              }
              case Some(tlist) => {

                val ttuple = TripSegTuple(oid, tlist.size + 1, LineSegment(LatLng(lat1, lng1), LatLng(lat2, lng2)), StringInterval(ot1, ot2))
                // existing trip
                // update semantic net
                // compare with latest update
                //              println("comparing new "+ttuple.tripSeg+" with latest"+tlist.last.tripSeg)
                // prev -> next
                //                val newRels = RelationEvaluator.evalRelation(tlist.last.tripSeg, ttuple.tripSeg)
                // prev <- next
                val newRels = RelationEvaluator.evalRelation(ttuple.tripSeg, tlist.last.tripSeg)

                val nodes = JsArray(JsObject("id" -> JsString("t" + oid), "label" -> JsString("Trip " + oid)),
                  JsObject("id" -> JsString("tseg" + ttuple.tripSegId.toLong), "label" -> JsString(ttuple.getLabel)),
                  JsObject("id" -> JsString("tseg" + tlist.last.tripSegId.toLong), "label" -> JsString(tlist.last.getLabel)))

                var builtRels = Iterable[Relation]()

                if (agg) {
                  
                  
                  // aggregate relations
                  val crossp = newRels.flatMap(r1 => {
                    newRels.flatMap(r2 => {
                      if (r1.eq(r2) && r1.dim == Dimensions.X) None
                      else Some((r1, r2))
                    })
                  })

//                  log.info(crossp.toString)

                  val aggRels = crossp.flatMap {
                    //eval north
                    case 
                    (During(Dimensions.X),After(Dimensions.Y,_)) |
                    (During(Dimensions.X),MeetInv(Dimensions.Y)) |
                    (Contains(Dimensions.X),After(Dimensions.Y,_)) |
                    (Contains(Dimensions.X),MeetInv(Dimensions.Y)) |
                    (Starts(Dimensions.X),After(Dimensions.Y,_)) |
                    (Starts(Dimensions.X),MeetInv(Dimensions.Y)) |
                    (StartsInv(Dimensions.X),After(Dimensions.Y,_)) |
                    (StartsInv(Dimensions.X),MeetInv(Dimensions.Y)) |
                    (Finishes(Dimensions.X),After(Dimensions.Y,_)) |
                    (Finishes(Dimensions.X),MeetInv(Dimensions.Y)) |
                    (FinishesInv(Dimensions.X),After(Dimensions.Y,_)) |
                    (FinishesInv(Dimensions.X),MeetInv(Dimensions.Y)) |
                    (Equals(Dimensions.X),After(Dimensions.Y,_)) |
                    (Equals(Dimensions.X),MeetInv(Dimensions.Y)) 
                    => {
                    	Some(North)
                    }
                    //eval south
                    case 
                    (During(Dimensions.X),Before(Dimensions.Y,_)) |
                    (During(Dimensions.X),Meet(Dimensions.Y)) |
                    (Contains(Dimensions.X),Before(Dimensions.Y,_)) |
                    (Contains(Dimensions.X),Meet(Dimensions.Y)) |
                    (Starts(Dimensions.X),Before(Dimensions.Y,_)) |
                    (Starts(Dimensions.X),Meet(Dimensions.Y)) |
                    (StartsInv(Dimensions.X),Before(Dimensions.Y,_)) |
                    (StartsInv(Dimensions.X),Meet(Dimensions.Y)) |
                    (Finishes(Dimensions.X),Before(Dimensions.Y,_)) |
                    (Finishes(Dimensions.X),Meet(Dimensions.Y)) |
                    (FinishesInv(Dimensions.X),Before(Dimensions.Y,_)) |
                    (FinishesInv(Dimensions.X),Meet(Dimensions.Y)) |
                    (Equals(Dimensions.X),Before(Dimensions.Y,_)) |
                    (Equals(Dimensions.X),Meet(Dimensions.Y)) 
                    => {
                    	Some(South)
                    }
                    //eval west
                    case 
                     (Before(Dimensions.X,_),During(Dimensions.Y)) |
                     (Before(Dimensions.X,_),Contains(Dimensions.Y)) |
                     (Before(Dimensions.X,_),Starts(Dimensions.Y)) |
                     (Before(Dimensions.X,_),StartsInv(Dimensions.Y)) |
                     (Before(Dimensions.X,_),Finishes(Dimensions.Y)) |
                     (Before(Dimensions.X,_),FinishesInv(Dimensions.Y)) |
                     (Before(Dimensions.X,_),Equals(Dimensions.Y)) |
                     (Meet(Dimensions.X),During(Dimensions.Y)) |
                     (Meet(Dimensions.X),During(Dimensions.Y)) |
                     (Meet(Dimensions.X),Contains(Dimensions.Y)) |
                     (Meet(Dimensions.X),Starts(Dimensions.Y)) |
                     (Meet(Dimensions.X),StartsInv(Dimensions.Y)) |
                     (Meet(Dimensions.X),Finishes(Dimensions.Y)) |
                     (Meet(Dimensions.X),FinishesInv(Dimensions.Y)) |
                     (Meet(Dimensions.X),Equals(Dimensions.Y)) 
                    => {
                      Some(West)
                    }
                    //eval east
                    case 
                    (After(Dimensions.X,_),During(Dimensions.Y)) |
                     (After(Dimensions.X,_),Contains(Dimensions.Y)) |
                     (After(Dimensions.X,_),Starts(Dimensions.Y)) |
                     (After(Dimensions.X,_),StartsInv(Dimensions.Y)) |
                     (After(Dimensions.X,_),Finishes(Dimensions.Y)) |
                     (After(Dimensions.X,_),FinishesInv(Dimensions.Y)) |
                     (After(Dimensions.X,_),Equals(Dimensions.Y)) |
                     (MeetInv(Dimensions.X),During(Dimensions.Y)) |
                     (MeetInv(Dimensions.X),During(Dimensions.Y)) |
                     (MeetInv(Dimensions.X),Contains(Dimensions.Y)) |
                     (MeetInv(Dimensions.X),Starts(Dimensions.Y)) |
                     (MeetInv(Dimensions.X),StartsInv(Dimensions.Y)) |
                     (MeetInv(Dimensions.X),Finishes(Dimensions.Y)) |
                     (MeetInv(Dimensions.X),FinishesInv(Dimensions.Y)) |
                     (MeetInv(Dimensions.X),Equals(Dimensions.Y)) 
                    => {
                      Some(East)
                    }
                    //eval northwest                    
                    case
                      (Before(Dimensions.X, _), After(Dimensions.Y,_)) |
                      (Before(Dimensions.X, _), MeetInv(Dimensions.Y)) |
                      (Before(Dimensions.X, _), Overlap(Dimensions.Y,_)) |
                      (Meet(Dimensions.X), After(Dimensions.Y,_)) |
                      (Meet(Dimensions.X), MeetInv(Dimensions.Y)) |
                      (Meet(Dimensions.X), Overlap(Dimensions.Y,_)) |
                      (Overlap(Dimensions.X,_),After(Dimensions.Y,_)) |
                      (Overlap(Dimensions.X,_),MeetInv(Dimensions.Y)) 
                      => {
                      Some(NorthWest)
                    }
                      // eval northeast
                    case
                      (After(Dimensions.X, _), After(Dimensions.Y,_)) |
                      (After(Dimensions.X, _), MeetInv(Dimensions.Y)) |
                      (After(Dimensions.X, _), Overlap(Dimensions.Y,_)) |
                      (MeetInv(Dimensions.X), After(Dimensions.Y,_)) |
                      (MeetInv(Dimensions.X), MeetInv(Dimensions.Y)) |
                      (MeetInv(Dimensions.X), Overlap(Dimensions.Y,_)) |
                      (Overlap(Dimensions.X,_),After(Dimensions.Y,_)) |
                      (Overlap(Dimensions.X,_),MeetInv(Dimensions.Y)) 
                      => {
                      Some(NorthEast)
                    }
                      //eval southwest
                    case
                      (Before(Dimensions.X, _), Before(Dimensions.Y,_)) |
                      (Before(Dimensions.X, _), Meet(Dimensions.Y)) |
                      (Before(Dimensions.X, _), Overlap(Dimensions.Y,_)) |
                      (Meet(Dimensions.X), Before(Dimensions.Y,_)) |
                      (Meet(Dimensions.X), Meet(Dimensions.Y)) |
                      (Meet(Dimensions.X), Overlap(Dimensions.Y,_)) |
                      (Overlap(Dimensions.X,_),Before(Dimensions.Y,_)) |
                      (Overlap(Dimensions.X,_),Meet(Dimensions.Y)) 
                      => {
                      Some(SouthWest)
                    }
                      //eval southeast 
                    case
                      (After(Dimensions.X, _), Before(Dimensions.Y,_)) |
                      (After(Dimensions.X, _), Meet(Dimensions.Y)) |
                      (After(Dimensions.X, _), Overlap(Dimensions.Y,_)) |
                      (MeetInv(Dimensions.X), Before(Dimensions.Y,_)) |
                      (MeetInv(Dimensions.X), Meet(Dimensions.Y)) |
                      (MeetInv(Dimensions.X), Overlap(Dimensions.Y,_)) |
                      (Overlap(Dimensions.X,_),Before(Dimensions.Y,_)) |
                      (Overlap(Dimensions.X,_),Meet(Dimensions.Y)) 
                      => {
                      Some(SouthEast)
                    }
                      // eval Left
                    case 
                    	(Before(Dimensions.X, _), _) |
                    	(Meet(Dimensions.X), _) => {
                    	  Some(Left)
                    	} 
                      // eval Right
                    case 
                    	(After(Dimensions.X, _), _) |
                    	(MeetInv(Dimensions.X), _) => {
                    	  Some(Right)
                    	} 
                      // eval Below
                    case 
                    	(_,Before(Dimensions.Y, _)) |
                    	(_,Meet(Dimensions.Y)) => {
                    	  Some(Below)
                    	} 
                      // eval Above
                    case 
                    	(After(Dimensions.X, _), _) |
                    	(MeetInv(Dimensions.X), _) => {
                    	  Some(Above)
                    	}
                      // eval Equal
                    case 
                    	(Equals(Dimensions.X), Equals(Dimensions.Y)) => {
                    	  Some(Equal)
                    	}
                      // eval Inside
                    case 
                    	(During(Dimensions.X), During(Dimensions.Y)) => {
                    	  Some(Inside)
                    	}
                      // eval Cover
                    case 
                    	(Contains(Dimensions.X), FinishesInv(Dimensions.Y)) |
                    	(Contains(Dimensions.X), StartsInv(Dimensions.Y)) |
                    	(Contains(Dimensions.X), Equals(Dimensions.Y)) 
                    	=> {
                    	  Some(Equal)
                    	}
                      // eval Overlap
                      // eval Touch
                      // eval Disjoint
                    case _ => {
                      None
                    }
                  }

                  //                log.info(aggRels.toString)

                  builtRels = aggRels.map(r => Relation(
                    Concept(ttuple.tripSegId.toLong, ttuple.getLabel),
                    Concept(tlist.last.tripSegId.toLong, tlist.last.getLabel),
                    r))

                } else {

                  builtRels = newRels.map(r => Relation(
                    Concept(ttuple.tripSegId.toLong, ttuple.getLabel),
                    Concept(tlist.last.tripSegId.toLong, tlist.last.getLabel),
                    r))

                }

                relations ++= builtRels

                val relarr = JsArray(builtRels.map(r => {
                  JsObject("from" -> JsString("tseg" + r.n1.id),
                    "to" -> JsString("tseg" + r.n2.id),
                    "label" -> JsString(r.rel.toString()))
                }).toVector ++
                  Array(JsObject("from" -> JsString("tseg" + ttuple.tripSegId.toLong),
                    "to" -> JsString("t" + oid),
                    "label" -> JsString("isPartOf"))))
                val out = JsObject("nodes" -> nodes, "rels" -> relarr).toString
                context.actorSelection(default_url) ! Forward[String](out)

                val ntlist = tlist ++ Array(ttuple)
                //              println("current trjectory : "+ntlist.toList)

                trajectories += (oid -> ntlist)
                //                val total = relations.size
                //              println(s"For $oid, $total : "+newRels)

              }
            }
          }

        })
    }
    case x @ _ => {
      log.info("got an unknown message " + x)
    }
  }
}

