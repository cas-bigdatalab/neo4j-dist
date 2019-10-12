package main.scala.version1.core.writenode

import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpointRef}
import org.neo4j.driver.v1.{Driver, Record}
import version1.core.ReadCypher2

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

class CypherReader(localDriver: Driver, cypher: String, context: RpcCallContext, refList:ArrayBuffer[RpcEndpointRef]) extends Runnable{

  override def run(): Unit = {
    val index = Random.nextInt(3)
    if (index == 2){
      var str = ""
      val session = localDriver.session()
      val res = session.run(cypher)
      while (res.hasNext) {
        val record: Record = res.next()
        str += record.toString + "\n"
      }
      context.reply(str)
      session.close()
    }else{
      val rpcRef = refList(index)
      val future: Future[String] = rpcRef.ask[String](ReadCypher2(cypher))
      future.onComplete {
        case scala.util.Success(value) => {
          println(s"read node finish: $value")
          context.reply(value)
        }
        case scala.util.Failure(e) => {
          println(s"read node error: $e")
          context.reply(e.getMessage)
        }
      }
      Await.ready(future, Duration.apply("30s"))
    }
  }
}

