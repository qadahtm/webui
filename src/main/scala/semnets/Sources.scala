package semnets

import akka.actor.{ Actor, ActorRef, Props }
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import java.net.InetSocketAddress
import akka.actor.ActorLogging
import akka.actor.ActorSystem

import scala.concurrent.duration._

object Sources {

}

object TcpClientTest {
  def main(args: Array[String]) : Unit= {
    implicit val system = ActorSystem("Test")
    implicit val ec = system.dispatcher
    implicit val log = system.log
    
    val remote_socket = new InetSocketAddress("localhost",1234)
    val printer = system.actorOf(Props(classOf[Printer]))
    
    val client = system.actorOf(Client.props(remote_socket, printer))
    
//    system.scheduler.scheduleOnce(10 seconds)(client ! "close")
//    system.awaitTermination
  }
}

class Printer extends Actor with ActorLogging {
  def receive = {
    case data:ByteString => log.info("received data : "+data.decodeString("UTF-8"))
  }
}

class SocketReciever(remote: InetSocketAddress) extends Actor with ActorLogging {

  def receive = {
    case _ => {}
  }
}

object Client {
  def props(remote: InetSocketAddress, replies: ActorRef) =
    Props(classOf[Client], remote, replies)
}

class Client(remote: InetSocketAddress, listener: ActorRef) extends Actor with ActorLogging{

  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote)

  def receive = {
    case CommandFailed(_: Connect) =>
      listener ! "failed"
      context stop self

    case c @ Connected(remote, local) =>
      listener ! c
      val connection = sender
      connection ! Register(self)
      context become {
        case data: ByteString => {
          connection ! Write(data)          
        }
        case CommandFailed(w: Write) => // O/S buffer was full
        case Received(data) => listener ! data
        case "close" => {
          connection ! Close
        }
        case _: ConnectionClosed => context stop self
      }
  }
}