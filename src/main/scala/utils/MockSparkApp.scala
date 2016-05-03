package utils

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.Socket
import java.io.PrintWriter
import java.util.concurrent._
import java.util.concurrent.Executor
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import spray.json.DefaultJsonProtocol
import spray.json._
import cruncher.ui.Catalog

object MockSparkStreamingApp extends App {

  val sampleTweetsFile = Helper.getConfig().getString("webserver.data.sampleTweets")
  var data = scala.io.Source.fromFile(sampleTweetsFile).getLines()

  val conf = new SparkConf().setMaster("local[2]").setAppName("MockCruncherDriver")
  val ssc = new StreamingContext(conf, Seconds(3))

  ssc.checkpoint("./checkpoints")

  val qn = Catalog.json_cqueries.keys.size
  if (qn > 0) {
    val qname = Catalog.json_cqueries.keys.toList(scala.util.Random.nextInt(qn))
    val qo = Catalog.json_cqueries.get(qname).get

  }

  val querystream_in = ssc.socketTextStream("localhost", 2016)

  val queryList = querystream_in.map(line => {

    val qjo = JsonParser(line).asJsObject
    val qid = qjo.fields.get("name").get.asInstanceOf[JsString].value
    var qtype = -1

    var kval = 0
    var fp: point = null
    var dist = 0f
    var toverlap: Boolean = false
    var insiderange: Box = null
    var keywords: String = null
    var tTime: String = null

    val plan = qjo.fields.get("plan").get.asJsObject
    var proj_children = plan.fields.get("children").get
    proj_children.asInstanceOf[JsArray].elements.foreach { cop =>
      {
        cop.asJsObject.fields.get("type").get.asInstanceOf[JsString].value match {
          case "select" => {
            val (_qtype, _toverlap, _keywords, _fp, _kval, _insiderange) = parseSelectNode(cop.asJsObject)
            if (_qtype != -1) qtype = _qtype
            if (_toverlap != false) toverlap = _toverlap
            if (_keywords != null) keywords = _keywords
            if (_fp != null) fp = _fp
            if (_kval != 0) kval = _kval
            if (_insiderange != null) insiderange = _insiderange

          }
          case "join" => {
            qtype = 2
            // check conditions for join

            val (_toverlap, _insiderange, _dist) = parseJoinNode(cop.asJsObject)
            if (_toverlap != false) toverlap = _toverlap
            if (_insiderange != null) insiderange = _insiderange
            if (_dist != 0f) dist = _dist

            // check one more level
            cop.asJsObject.fields.get("children").get.asInstanceOf[JsArray].elements.foreach { _cop =>
              {
                _cop.asJsObject.fields.get("type").get.asInstanceOf[JsString].value match {
                  case "select" => {
                    val (__qtype, __toverlap, __keywords, __fp, __kval, __insiderange) = parseSelectNode(cop.asJsObject)
                    if (__toverlap != false) toverlap = __toverlap
                    if (__keywords != null) keywords = __keywords
                    if (__fp != null) fp = __fp
                    if (__kval != 0) kval = __kval
                    if (__insiderange != null) insiderange = __insiderange

                  }
                }
              }
            }
          }
          case "source" => {
            // do nothing
          }
          case _ => {}

        }
      }
    }

    val q = query(qid, qtype, fp, kval, dist, toverlap, insiderange, keywords, tTime, qjo)
    (1, Seq(q))
  })

  val queries = queryList.updateStateByKey[Seq[query]](updateQueriesState _)

  queries.print()

  queries.foreachRDD(rdd => {
    rdd.foreach {
      case (i, qs) => {
        val q = qs(0)
        val uisocket = new Socket("localhost", 2016)
        val output_out = new PrintWriter(uisocket.getOutputStream(), true);
        try {

          val qname = q.jobj.fields.get("name").get.asInstanceOf[JsString].value
          val cv = q.jobj.fields.get("currentView").get
          val qcolor = q.jobj.fields.get("outputColor").get
          val bounds = cv.asJsObject.getFields("north", "west", "south", "east").map { _.asInstanceOf[JsNumber].value }
          val rlng = bounds(1).toDouble + (scala.math.abs(bounds(3).toDouble - bounds(1).toDouble) * scala.util.Random.nextDouble())
          val rlat = bounds(2).toDouble + (scala.math.abs(bounds(0).toDouble - bounds(2).toDouble) * scala.util.Random.nextDouble())

          var txt = ""
          var oid = ""
          if (data.hasNext) {
            val arr = data.next().split(",")
            txt = arr(5)
            oid = arr(0)
          } else {
            data = scala.io.Source.fromFile(sampleTweetsFile).getLines()
            val arr = data.next().split(",")
            txt = arr(5)
            oid = arr(0)
          }

          val resp = JsObject("name" -> JsString(qname),
            "type" -> JsString("output"),
            "oid" -> JsString(oid),
            "tag" -> JsString("+"),
            "outputColor" -> qcolor,
            "point" -> JsObject("lat" -> JsNumber(rlat), "lng" -> JsNumber(rlng)),
            "text" -> JsString(txt))

          output_out.println(resp.toString())

          output_out.flush()
          output_out.close()
          uisocket.close()
        } catch {
          case t: Throwable => None
        }
      }
    }

  })

  ssc.start()
  ssc.awaitTermination()

  def updateQueriesState(newqueries: Seq[Seq[query]], prevState: Option[Seq[query]]): Option[Seq[query]] = {
    prevState match {
      case Some(qs) => {
        if (newqueries.size == 0) {
          Some(qs)
        } else if (newqueries.size == 1) {
          val nq = newqueries(0)
          val nqs = qs.filter(!nq.contains(_))
          Some(nqs ++ nq)
        } else {
          val nnqs = newqueries.reduce((sq1, sq2) => {
            val fsq2 = sq2.filter(sq1.contains(_))
            sq1 ++ fsq2
          })
          val nqs = qs.filter(!nnqs.contains(_))
          Some(nqs ++ nnqs)
        }
      }
      case None => {
        Some((newqueries.reduce(_ ++ _)))
      }
    }
  }

  def parseJoinNode(cop: JsObject) = {
    var toverlap: Boolean = false
    var insiderange: Box = null
    var dist = 0f

    cop.fields.get("conditions").get.asInstanceOf[JsArray].elements.foreach { scond =>
      {
        scond.asJsObject.fields.get("op").get.asInstanceOf[JsString].value match {
          case "OVERLAPS" => {
            toverlap = true
          }

          case "INSIDE" => {
            val n = scond.asJsObject.fields.get("rhs").get.asJsObject.fields.get("north").get.asInstanceOf[JsNumber].value.toDouble
            val s = scond.asJsObject.fields.get("rhs").get.asJsObject.fields.get("south").get.asInstanceOf[JsNumber].value.toDouble
            val w = scond.asJsObject.fields.get("rhs").get.asJsObject.fields.get("west").get.asInstanceOf[JsNumber].value.toDouble
            val e = scond.asJsObject.fields.get("rhs").get.asJsObject.fields.get("east").get.asInstanceOf[JsNumber].value.toDouble
            insiderange = Box(w, s, e, n)
          }

          case "WITHIN_DISTANCE" => {
            dist = scond.asJsObject.fields.get("kval").get.asInstanceOf[JsNumber].value.toFloat
          }
        }

      }
    }

    (toverlap, insiderange, dist)
  }

  def parseSelectNode(cop: JsObject) = {
    var toverlap: Boolean = false
    var keywords: String = null
    var fp: point = null
    var kval: Int = 0
    var insiderange: Box = null
    var qtype: Int = 0

    cop.fields.get("conditions").get.asInstanceOf[JsArray].elements.foreach { scond =>
      {
        scond.asJsObject.fields.get("op").get.asInstanceOf[JsString].value match {
          case "OVERLAPS" => {
            toverlap = true
            keywords = scond.asJsObject.fields.get("rhs").get.asInstanceOf[JsArray].elements.map { _.asInstanceOf[JsString].value }.mkString(" ")

          }
          case "KNN" => {
            val fpv = scond.asJsObject.fields.get("rhs").get.asJsObject.fields.get("val").get.asJsObject
            val fplat = fpv.fields.get("lat").get.asInstanceOf[JsNumber].value.toDouble
            val fplng = fpv.fields.get("lng").get.asInstanceOf[JsNumber].value.toDouble
            fp = point(fplng, fplat)
            kval = scond.asJsObject.fields.get("kval").get.asInstanceOf[JsNumber].value.toIntExact
            qtype = 1
          }

          case "INSIDE" => {
            val n = scond.asJsObject.fields.get("rhs").get.asJsObject.fields.get("north").get.asInstanceOf[JsNumber].value.toDouble
            val s = scond.asJsObject.fields.get("rhs").get.asJsObject.fields.get("south").get.asInstanceOf[JsNumber].value.toDouble
            val w = scond.asJsObject.fields.get("rhs").get.asJsObject.fields.get("west").get.asInstanceOf[JsNumber].value.toDouble
            val e = scond.asJsObject.fields.get("rhs").get.asJsObject.fields.get("east").get.asInstanceOf[JsNumber].value.toDouble
            insiderange = Box(w, s, e, n)
          }
        }

      }
    }
    (qtype, toverlap, keywords, fp, kval, insiderange)
  }

  case class point(x: Double, y: Double) extends Serializable {
    override def toString() = {

      "X-Cor:" + x.toString() + " Y-Coordniate: " + y.toString()
    }
  }

  case class Box(x1: Double, y1: Double, x2: Double, y2: Double) extends Serializable {
    override def toString() = {

      "X-Cor:" + x1.toString() + " Y-Coordniate: " + y1.toString()
    }

    def Inside(p: point): Boolean = {
      if (p.x > x1 && p.x < x2 && p.y > y1 && p.y < y2) {
        return true
      }
      return false
    }

  }

  case class tweet(ID: Double, location: point, tText: String, tTime: String) extends Serializable {

    override def equals(that: Any): Boolean = {
      true
    }

  }

  case class query(ID: String, Type: Int, focalpoint: point, K: Int, Distance: Float, TextOverlap: Boolean, Range: Box, Keywords: String, tTime: String, jobj: JsObject) extends Serializable {

    override def equals(that: Any): Boolean = {
      true
    }

    def evaluate(t: tweet): Boolean = {
      if (Range.Inside(t.location) && (t.tText.toLowerCase.contains(Keywords.toLowerCase.toCharArray))) {
        return true
      }
      false
    }

  }

}

object MockSparkApp extends App {

  val uisocket = new Socket("localhost", 2016)
  val queries_in = new BufferedReader(
    new InputStreamReader(uisocket.getInputStream()));
  val output_out = new PrintWriter(uisocket.getOutputStream(), true);

  val ec = Executors.newFixedThreadPool(1)

  ec.execute(new QueryReceiver(queries_in))

  while (true) {

    // write some output
    for (i <- 1 to 10) {
      output_out.println(s"msg ${i}")
    }

    output_out.flush()

    //    println("sleeping for one second")
    Thread.sleep(1000)
  }

}

class QueryReceiver(queries_in: BufferedReader) extends Runnable {
  def run() {
    // get a query
    while (true) {

      if (queries_in.ready()) {
        println("getting a query")
        val qline = queries_in.readLine()
        println(s"got a query ${qline}")
      }

      Thread.sleep(500)
    }
  }
}