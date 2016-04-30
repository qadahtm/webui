package cruncher.ui

import java.util.concurrent.LinkedBlockingQueue

object CruncherDataBuffer {
  
  val outputQueue = new LinkedBlockingQueue[String](10000)
  val queryQueue = new LinkedBlockingQueue[String](10000)
  
  def addOutput(ans:String) = outputQueue.add(ans)
  
  def getOutputSync() = outputQueue.take()
  
  def getOutputASync() = outputQueue.poll()
  
  
  def addQuery(q:String) = queryQueue.add(q)
  
  def getQuerySync() = queryQueue.take()
  
  def getQueryASync() = queryQueue.poll()

}