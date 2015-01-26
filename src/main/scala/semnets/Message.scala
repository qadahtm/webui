package semnets

import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat
import spray.json._


object SemNetJsonProtocol extends DefaultJsonProtocol {
  
  implicit object ConceptJsonFormat extends RootJsonFormat[Concept] {
    def write(c: Concept) = JsObject(
      "id" -> JsNumber(c.id),
      "lable" -> JsString(c.label)
    )
    def read(value: JsValue) = {
//      value.asJsObject.getFields("name", "red", "green", "blue") match {
//        case Seq(JsString(name), JsNumber(red), JsNumber(green), JsNumber(blue)) =>
//          new Color(name, red.toInt, green.toInt, blue.toInt)
//        case _ => throw new DeserializationException("Color expected")
//      }
      null
    }
  }
  
  implicit object RelationJsonFormat extends RootJsonFormat[Relation] {
    def write(r:Relation) = JsObject(
        "nid1" -> JsNumber(r.n1.id),
        "nid2" -> JsNumber(r.n2.id),        
        "label" -> JsString(r.rel.toString)
        )
    def read(value:JsValue) = null
  }
}

case class Concept(val id:Long, 
    val label:String) extends Serializable {
  override def equals(other: Any) = other match {
    case that: Concept => that.id == this.id
    case _            => false
  }
  override def hashCode = id.##
}

case class Relation(n1: Concept,n2:Concept,rel:AbstractRelation) extends Serializable

case class RelsUpdate(rels:Array[Relation])
case class SemNetState(val concepts:Iterable[Concept],val relations:Iterable[Relation])

object GetSemNetState

abstract class Tuple

case class LineSegment(start:LatLng,end:LatLng)
case class TripSegTuple(tripId:Long, tripSegId:Long,tripSeg:LineSegment,temporal:StringInterval) 
	extends Tuple {
  def getLabel = "TripSeg:"+tripSegId
}
// trip tuples capture trips that may have multiple segments
case class TripTuple(tripId:Long,tripSegments:List[LineSegment],temporal:StringInterval) extends Tuple

case class LocationPointUpdate(oid:String,spatial:LatLng,temporal:Long) extends Tuple
case class LocationLineUpdate(oid:String,spatial:LineSegment,temporal:TemporalInterval) extends Tuple

//case class SpatialAttributes(lat:Double, lng:Double)
case class TemporalInterval(sts:Long,ets:Long)
case class StringInterval(start:String,end:String)

case class Rule(head:String, body:List[String])
case class GraphRule()

abstract class AbstractRelation extends Serializable
abstract class TopologicalRelation extends AbstractRelation
object Equal extends TopologicalRelation {
   override def toString = "Equal"
}
object Inside extends TopologicalRelation {
   override def toString = "Inside"
}
object Cover extends TopologicalRelation {
   override def toString = "Cover"
}
object Overlap extends TopologicalRelation {
   override def toString = "Overlap"
}
object Touch extends TopologicalRelation {
   override def toString = "Touch"
}
object Disjoint extends TopologicalRelation {
   override def toString = "Disjoint"
}


abstract class DirectionalRelation extends AbstractRelation 
object North extends DirectionalRelation {
   override def toString = "North"
}
object South extends DirectionalRelation {
   override def toString = "South"
}
object West extends DirectionalRelation {
   override def toString = "West"
}
object East extends DirectionalRelation {
   override def toString = "East"
}
object NorthWest extends DirectionalRelation {
   override def toString = "NorthWest"
}
object SouthWest extends DirectionalRelation {
   override def toString = "SouthWest"
}
object NorthEast extends DirectionalRelation {
   override def toString = "NorthEast"
}
object SouthEast extends DirectionalRelation {
   override def toString = "SouthEast"
}
object Left extends DirectionalRelation {
   override def toString = "Left"
}
object Right extends DirectionalRelation {
   override def toString = "Right"
}
object Below extends DirectionalRelation {
   override def toString = "Below"
}
object Above extends DirectionalRelation {
   override def toString = "Above"
}


abstract class IntervalRelation() extends AbstractRelation {
  def dim:String
}
case class Before(val dim: String, val delta: Double) extends IntervalRelation 
case class After(val dim: String, val delta: Double) extends IntervalRelation
case class Contains(val dim: String) extends IntervalRelation
case class During(val dim: String) extends IntervalRelation
case class Meet(val dim: String) extends IntervalRelation
case class MeetInv(val dim: String) extends IntervalRelation
case class Overlap(val dim: String, val delta: Double) extends IntervalRelation
case class Starts(val dim: String) extends IntervalRelation
case class StartsInv(val dim: String) extends IntervalRelation
case class Finishes(val dim: String) extends IntervalRelation
case class FinishesInv(val dim: String) extends IntervalRelation
case class Equals(val dim: String) extends IntervalRelation

case class Interval(s:Double,e:Double)
case class LatLng(lat:Double,lng:Double)
case class SpatialObject(x:Interval,y:Interval)

object Dimensions {
  def dims: List[String] = List(X,Y,T)
  val X: String = "x"
  val Y: String = "y"
  val T: String = "t"
}

object RelationEvaluator {
  
  // Interval Relation Evaluation
  val _eps = 0.0//0000000001
  
  def double_eq(a:Double, b:Double):Boolean = double_eq(a,b,_eps)
  def double_eq(a:Double, b:Double, eps:Double) :Boolean = (math.abs(a-b) <= eps)
  
  def double_gt(a:Double, b:Double):Boolean = double_gt(a,b,_eps)
  def double_gt(a:Double, b:Double, eps:Double):Boolean = (a+eps) > b
  
  def double_lt(a:Double, b:Double): Boolean = double_lt(a,b,_eps)
  def double_lt(a:Double, b:Double, eps:Double): Boolean = (a-eps) < b
  
  def evalRelation(seg1:LineSegment, seg2:LineSegment):Iterable[IntervalRelation] = {
    evalRelation(Dimensions.X,
        seg1.start.lng,seg1.end.lng,
        seg2.start.lng,seg2.end.lng) ++ 
    evalRelation(Dimensions.Y,
        seg1.start.lat,seg1.end.lat,
        seg2.start.lat,seg2.end.lat)
  }
  
//  def evalRelation(dim: String, v1: Double, v2: Double, x1:Double, x2:Double, y1:Double, y2:Double): Iterable[IntervalRelation] = {
//    dim match {
//      case Dimensions.X => {
//        evalRelation(dim,v1, v2, x1, x2).toIterable
//      }
//      case Dimensions.Y => {
//        evalRelation(dim,v1, v2, y1, y2).toIterable
//      }
//      case _ => List()
//    }
//    
//  }
	//  def getMBR = SpatialObject(Interval(x1,x2),Interval(y1,y2))
  
//  def evalRelation(dim: String,s1: Double, e1: Double, s2: Double, e2: Double): Option[IntervalRelation] = {
//    if (double_lt(e1 , s2)) Some(Before(dim, (s2 - e1))) //TODO consider eps
//    else if (double_eq(e1, s2)) Some(Meet(dim))
//    else if (double_eq(e2, s1)) Some(MeetInv(dim))
//    else if ((double_lt(s1 , s2) && double_lt(s2 , e1) && double_lt(e1 , e2)) || 
//        double_gt(s1 , s2) && double_gt(s2 , e1) && double_gt(e1 , e2)) Some(Overlap(dim, (s2 - s1)))
//    else if (double_gt(s1 , s2) && double_lt(e1 , e2)) Some(During(dim)) // inverse of contains
//    else if (double_lt(s1 , s2) && double_gt(e1 , e2)) Some(Contains(dim))
//    else if (double_eq(s1 , s2) && double_lt(e1 , e2)) Some(Starts(dim))
//    else if (double_eq(s1 , s2) && double_gt(e1 , e2)) Some(StartsInv(dim))
//    else if (double_gt(s1 , s2) && double_eq(e1 , e2)) Some(Finishes(dim))
//    else if (double_lt(s1 , s2) && double_eq(e1 , e2)) Some(FinishesInv(dim))
//    else if (double_eq(s1 , s2) && double_eq(e1 , e2)) Some(Equals(dim))
//    else None
//  }
  
  def evalRelation(dim: String,s1: Double, e1: Double, s2: Double, e2: Double): Option[IntervalRelation] = {
//    println(s"eval($dim): ($s1,$e1) and ($s2,$e2)")
    if (e1 < s2) Some(Before(dim, (s2 - e1))) //TODO consider eps
    else if (e2 < s1) Some(After(dim,(s1-e2)))
    else if ((e1 == s2)) Some(Meet(dim))
    else if ((e2== s1)) Some(MeetInv(dim))
    else if (((s1 < s2) && (s2 < e1) && (e1 < e2)) || 
        (s1 > s2) && (s2 > e1) && (e1 > e2)) Some(Overlap(dim, (s2 - s1)))
    else if ((s1 > s2) && (e1 < e2)) Some(During(dim)) // inverse of contains
    else if ((s1 < s2) && (e1 > e2)) Some(Contains(dim))
    else if ((s1 == s2) && (e1 < e2)) Some(Starts(dim))
    else if ((s1 == s2) && (e1 > e2)) Some(StartsInv(dim))
    else if ((s1 > s2) && (e1 == e2)) Some(Finishes(dim))
    else if ((s1 < s2) && (e1 == e2)) Some(FinishesInv(dim))
    else if ((s1 == s2) && (e1 == e2)) Some(Equals(dim))
    else None
  }
}

object BerlinMODLatLngConverter {

  // The following values where obtained from BBBike <https://github.com/eserte/bbbike>
  val x0 = -780761.760862528
  val x1 = 67978.2421158527
  val x2 = -2285.59137120724
  val y0 = -5844741.03397902
  val y1 = 1214.24447469596
  val y2 = 111217.945663725

  def getLatLng(newx: Double, newy: Double) = {
    val lat = ((newx - x0) * y1 - (newy - y0) * x1) / (x2 * y1 - x1 * y2)
    val lng = ((newx - x0) * y2 - (newy - y0) * x2) / (x1 * y2 - y1 * x2)
    (lat, lng)
  }
}