package utils

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.Socket
import java.io.PrintWriter

import java.util.concurrent._

import java.util.concurrent.Executor

/**
 * @author qadahtm
 */
object MockSparkApp extends App {
  
  val uisocket = new Socket("localhost", 9981)
  val queries_in = new BufferedReader(
                        new InputStreamReader(uisocket.getInputStream()));
  val output_out = new PrintWriter(uisocket.getOutputStream(), true);
  
  
  val ec = Executors.newFixedThreadPool(1)
  
  ec.execute(new QueryReceiver(queries_in))
    
  while (true){
    
    // write some output
    for (i <- 1 to 10){
      output_out.println(s"msg ${i}")
    }
    
    output_out.flush()
    
    
//    println("sleeping for one second")
    Thread.sleep(1000)
  }
  
  
}

class QueryReceiver(queries_in:BufferedReader) extends Runnable {
  def run(){
    // get a query
    while (true){
      
      if (queries_in.ready()){
        println("getting a query")
        val qline = queries_in.readLine()
        println(s"got a query ${qline}") 
      }       
      
      Thread.sleep(500)
    }
  }
}