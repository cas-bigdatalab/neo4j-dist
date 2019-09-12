package version0.core

import java.util

import net.neoremind.kraps.rpc.{RpcCallContext, RpcEnv}
import version0.zookeeper.ZkClient

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class CypherWriter(cypher: String, context: RpcCallContext, rpcEnv: RpcEnv, account:String, password:String, connectStr:String, sessionTimeout:Int) extends Runnable{
  //connect to other rpc and receive the result of writing

  var hosts:util.ArrayList[String] = _
  var driverList = ArrayBuffer[myDriver]()

  override def run(): Unit = {
    //get hosts from zookeeper
    val client = new ZkClient(connectStr, sessionTimeout)
    client.getConnect()
    hosts = client.getChildren

    // distribute RPC，parallel execution
    var count = 0
    val futureArray = ArrayBuffer[Future[String]]()

    // send to all nodes to write
    for (i <- 0.until(hosts.size())) {
      val ip = hosts.get(i)
      try {
        val driver = new myDriver("bolt://" + ip + ":7687", account, password)
        driverList += driver
        val res = driver.userWriteCypher(cypher)
        if (res == "success") count += 1
      }catch{
        case e:Exception =>{
          println(e.getMessage)
          context.reply("Start driver failed, check your account and password ! ")

        }
      }
    }

    if (count == hosts.size()) {
      var countSuccess = 0
      for (driver <- driverList) {
        try{
          driver.driverSuccess()
          countSuccess += 1
        }
      }
      if (countSuccess == driverList.length) context.reply("Write Success")
      else{
        context.reply("Write Failed")
        for (driver <- driverList) {
          try driver.driverFailed()
        }
      }
    }

  }
}
