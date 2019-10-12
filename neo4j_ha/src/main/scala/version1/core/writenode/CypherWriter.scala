package main.scala.version1.core.writenode

import net.neoremind.kraps.rpc.RpcCallContext
import org.neo4j.driver.v1.{Driver, Transaction}

import scala.collection.mutable.ArrayBuffer
class CypherWriter(driverList:ArrayBuffer[Driver], cypher:String, context:RpcCallContext)extends Runnable{

  override def run(): Unit = {
    // check success nodes
    val txList = new ArrayBuffer[Transaction]()

    // send to all nodes to write
    try{
      driverList.foreach(
        driver => {
          val tx = driver.session().beginTransaction()
          tx.run(cypher)
          txList += tx
        }
      )
      txList.foreach(
        tx =>{
          tx.success()
          tx.close()
        }
      )
      context.reply("Write Success")
    }catch {
      case e:Exception =>{
        txList.foreach(
          tx =>{
            tx.failure()
            tx.close()
          }
        )
        context.reply("Write Failed")
      }
    }
  }
}
