package version1.core

import net.neoremind.kraps.rpc.RpcCallContext
import org.neo4j.driver.v1.{Driver, Record}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class CypherReader(driverList: ArrayBuffer[Driver], cypher: String, context: RpcCallContext) extends Runnable{

  override def run(): Unit = {
    var str = ""
    val index = Random.nextInt(3)
    val session = driverList(index).session()
    val res = session.run(cypher)
    while (res.hasNext) {
      val record: Record = res.next()
      str += record.toString + "\n"
    }
    session.close()
    context.reply(str)
  }
}
