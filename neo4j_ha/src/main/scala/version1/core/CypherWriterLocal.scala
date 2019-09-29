package version1.core

import net.neoremind.kraps.rpc.RpcCallContext
import org.neo4j.driver.v1.{Driver, Transaction}

class CypherWriterLocal(driver:Driver, cypher:String, context:RpcCallContext)extends Runnable{

  override def run(): Unit = {
    var tx:Transaction = null
    // send to all nodes to write
    try{
      tx = driver.session().beginTransaction()
      tx.run(cypher)
      tx.success()
      tx.close()

      context.reply("Write Success")
    }catch {
      case e:Exception =>{
        tx.failure()
        tx.close()
        context.reply("Write Failed")
      }
    }
  }
}
