package version1.test

import java.util.concurrent.locks.ReentrantLock

import scala.actors.threadpool.{ExecutorService, Executors}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object RunClient {
  def main(args: Array[String]): Unit = {
    val threadPool:ExecutorService=Executors.newFixedThreadPool(25)
//    val myClient = new client
    val lock = new ReentrantLock(true)
    val inputFile = Source.fromFile("C:\\Users\\liam gao\\Desktop\\cypher_read.txt", enc = "GBK")
    val lines = inputFile.getLines()
    val cyphers = ArrayBuffer[String]()
    for(line <- lines){
      cyphers += line
    }
    var count = 1
    val startTime = System.currentTimeMillis()

    cyphers.foreach(
      cypher =>{
        println(count)
        threadPool.execute(new client(cypher, startTime))
        count += 1
      }
    )
  }
}
