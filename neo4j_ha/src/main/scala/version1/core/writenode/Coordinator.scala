package main.scala.version1.core.writenode

import java.net.InetAddress

import main.scala.version1.zookeeper.ZkClient
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv, RpcEnvClientConfig, RpcEnvServerConfig}
import org.neo4j.driver.v1.{AuthTokens, Driver, GraphDatabase}
import version1.core.{ReadCypher, WriteCypher}
import version1.setting.MySettings

import scala.actors.threadpool.{ExecutorService, Executors}
import scala.collection.mutable.ArrayBuffer


object Coordinator {
  def main(args: Array[String]): Unit = {
    val host = InetAddress.getLocalHost.getHostAddress
    val config = RpcEnvServerConfig(new RpcConf(), "WriteNode", host, 6668)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val coordinatorEndpoint: RpcEndpoint = new Coordinator(rpcEnv)
    rpcEnv.setupEndpoint("WriteNode", coordinatorEndpoint)
    rpcEnv.awaitTermination()
  }
}

class Coordinator(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  val threadPool: ExecutorService = Executors.newFixedThreadPool(100)
  val settings = new MySettings
  val hostIp:String = InetAddress.getLocalHost.getHostAddress
  val localDriver:Driver = GraphDatabase.driver(s"bolt://localhost:7687",
    AuthTokens.basic(settings.account, settings.password))

  val driverList: ArrayBuffer[Driver] = ArrayBuffer[Driver]()
  driverList += localDriver

  val rpcRefList:ArrayBuffer[RpcEndpointRef] = ArrayBuffer[RpcEndpointRef]()

  override def onStart(): Unit = {
    println("Coordinator is initializing....")

    try {
      val client = new ZkClient(settings.connectString, settings.sessionTimeout)
      client.getConnect()
      val hosts = client.getChildren

      val rpcConf = new RpcConf()
      for (i <- 0 until hosts.size()) {
        val ip = hosts.get(i)
        if (ip != hostIp){
          println(ip)
          val driver = GraphDatabase.driver(s"bolt://$ip:7687", AuthTokens.basic(settings.account, settings.password))
          driverList += driver
          val config = RpcEnvClientConfig(rpcConf, hostIp)
          val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
          val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(ip, 6668),
            "ReadNode")
          rpcRefList += endPointRef
        }
      }
      println("Initializing SUCCESS!!!")
    } catch {
      case e: Exception => {
        System.out.println("Please start your Zookeeper Server!!!" + e.getMessage)
        rpcEnv.shutdown()
      }
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case WriteCypher(cypher: String) => {
      //      TODO: check the number of online nodes are the same as zk's nodes?
      threadPool.execute(new CypherWriter(driverList, cypher, context))
    }

    case ReadCypher(cypher: String) => {
      //TODO: now use a write node as read node, latter not use write node
      threadPool.execute(new CypherReader(localDriver, cypher, context, rpcRefList))
    }
  }
}

