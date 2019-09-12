package version0.core

import net.neoremind.kraps.rpc.RpcCallContext
import org.neo4j.driver.v1._

class myDriver(url: String, user: String, password: String) {
  val driver:Driver = GraphDatabase.driver(url, AuthTokens.basic(user, password))
  val session = driver.session()
  var tx:Transaction = null

  def userWriteCypher(cypher_str:String): String = {
    tx = session.beginTransaction()
    tx.run(cypher_str)
    "success"
  }

  def driverSuccess(): Unit ={
    tx.success()
    tx.close()
    driver.close()
  }

  def driverFailed(): Unit ={
    tx.failure()
    tx.close()
    driver.close()
  }

  def userReadCypher(cypher_str:String, context: RpcCallContext): Unit = {
    var str = ""
    try {
      val session = driver.session()
      try {
        val result:StatementResult = session.run(cypher_str)
        while (result.hasNext){
          val record:Record = result.next()

          str += record.get("name") + " " + "age: " + record.get("age") + "\n"
        }
        session.close()
        driver.close()
        context.reply(str)
      }catch {
        case ex: Exception => {
          println(ex.getMessage())
          driver.close()
          context.reply(ex.getMessage)
        }
      }
    }catch {
      case ex:Exception=>{
        println(ex.getMessage())
        driver.close()
        context.reply(ex.getMessage)
      }
    }
  }
}
