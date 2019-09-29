package version1.core

import java.net.InetAddress
import java.util

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}
import org.neo4j.driver.v1.{AccessMode, AuthTokens, Driver, GraphDatabase, Session, Transaction}
import version1.setting.MySettings
import version1.zookeeper.ZkClient

import scala.actors.threadpool.{ExecutorService, Executors}
import scala.collection.mutable.ArrayBuffer


object Coordinator {
  def main(args: Array[String]): Unit = {
    val host = InetAddress.getLocalHost.getHostAddress
    val config = RpcEnvServerConfig(new RpcConf(), "server", host, 6668)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val coordinatorEndpoint: RpcEndpoint = new Coordinator(rpcEnv)
    rpcEnv.setupEndpoint("server", coordinatorEndpoint)
    rpcEnv.awaitTermination()
  }
}

class Coordinator(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  val threadPool: ExecutorService = Executors.newFixedThreadPool(25)
  val settings = new MySettings

  var hosts: util.ArrayList[String] = _
  var driverList = ArrayBuffer[Driver]()
  var localDriver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123123"))

  override def onStart(): Unit = {
    println("Coordinator is initializing....")
    //TODO: check the status of ZK
    try {
      if (hosts == null) {
        val client = new ZkClient(settings.connectString, settings.sessionTimeout)
        client.getConnect()
        hosts = client.getChildren
        println(hosts)
        for (i <- 0 until hosts.size()) {
          val ip = hosts.get(i)
          val driver:Driver = GraphDatabase.driver("bolt://" + ip + ":7687", AuthTokens.basic(settings.account, settings.password))
          driverList += driver
        }
      }
      print("Initializing SUCCESS!!!")
    } catch {
      case e: Exception => {
        System.out.println("Please start your Zookeeper Server!!!")
        rpcEnv.shutdown()
      }
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case WriteCypher(cypher: String) => {
      //      TODO: check the number of online nodes are the same as zk's nodes?
      threadPool.execute(new CypherWriter(driverList, cypher, context))
      //      threadPool.execute(new CypherWriterLocal(localDriver, cypher, context))
    }

    case ReadCypher(cypher: String) => {
      threadPool.execute(new CypherReader(driverList, cypher, context))
      //      threadPool.execute(new CypherReaderLocal(localDriver, cypher, context))

    }
  }

}
