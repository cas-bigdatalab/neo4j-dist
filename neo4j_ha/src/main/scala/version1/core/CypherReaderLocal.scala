package version1.core


import net.neoremind.kraps.rpc.RpcCallContext
import org.neo4j.driver.v1.{Driver, Record}


class CypherReaderLocal(driver: Driver, cypher: String, context: RpcCallContext) extends Runnable{

  override def run(): Unit = {
    var str = ""
    var count = 0
    // send to all nodes to write
    val session = driver.session()
    val res = session.run(cypher)
    while (res.hasNext) {
      val record: Record = res.next()
      str += record.toString + "\n"
    }
    session.close()
    context.reply(str)
  }
}
