package main.scala.version1.core.readnode

import java.net.InetAddress

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}
import org.neo4j.driver.v1.{AuthTokens, Driver, GraphDatabase}
import version1.core.ReadCypher2
import version1.setting.MySettings

import scala.actors.threadpool.{ExecutorService, Executors}


object Coordinator {
  def main(args: Array[String]): Unit = {
    val host = InetAddress.getLocalHost.getHostAddress
    val config = RpcEnvServerConfig(new RpcConf(), "ReadNode", host, 6668)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
    val coordinatorEndpoint: RpcEndpoint = new Coordinator(rpcEnv)
    rpcEnv.setupEndpoint("ReadNode", coordinatorEndpoint)
    rpcEnv.awaitTermination()
  }
}

class Coordinator(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  val threadPool: ExecutorService = Executors.newFixedThreadPool(100)
  val settings = new MySettings
  val hostIp:String = InetAddress.getLocalHost.getHostAddress
  val localDriver:Driver = GraphDatabase.driver(s"bolt://localhost:7687",
    AuthTokens.basic(settings.account, settings.password))

  override def onStart(): Unit = {
    println("Coordinator is started....")

  }

  override def receiveAndReply(context_to_master: RpcCallContext): PartialFunction[Any, Unit] = {

    case ReadCypher2(cypher: String) => {
      //TODO: now use a write node as read node, latter not use write node
      threadPool.execute(new CypherReader(localDriver, cypher, context_to_master))
    }
  }
}
