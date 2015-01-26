package semnets

import scalax.collection._
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._
import scalax.collection.edge._, scalax.collection.edge.Implicits._

import scalax.collection.io.json._
import scalax.collection.io.json.descriptor.predefined.{ DiHyper, Di }

object Playground {
  // node types: Book, Author
  sealed trait Library
  case class Book(val title: String,
    val isbn: String) extends Library
  case class Author(val surName: String,
    val firstName: String) extends Library
    
    val bookDescriptor = new NodeDescriptor[Book](typeId = "Books") {
    def id(node: Any) = node match {
      case Book(_, isbn) => isbn
    }
  }
  val authorDescriptor = new NodeDescriptor[Author](typeId = "Authors"){
    def id(node: Any) = node match {
      case Author(surName, firstName) => "" + surName(0) + firstName(0) }
    }
  
  object Named {
    val descriptor = new Descriptor[Library](
        defaultNodeDescriptor = authorDescriptor,
        defaultEdgeDescriptor = DiHyper.descriptor[Library](),
        namedNodeDescriptors  = Seq(bookDescriptor),
        namedEdgeDescriptors  = Seq(Di.descriptor[Library]())
    )
  }

  def main(args: Array[String]) = {

    // node data: 2 books, 4 authors
    val (programming, inDepth) = (
      Book("Programming in Scala", "978-0-9815316-2-5"),
      Book("Scala in Depth", "978-1-9351827-0-2"))
    val (martin, lex, bill, josh) = (
      Author("Odersky", "Martin"),
      Author("Spoon", "Lex"),
      Author("Venners", "Bill"),
      Author("Suereth", "Joshua D."))

    // graph with 2 authorships
    val library = Graph[Library, HyperEdge](
      programming ~> martin,
      inDepth ~> josh)
      
      println(library.toJson(Named.descriptor))
  }
  
  
}
