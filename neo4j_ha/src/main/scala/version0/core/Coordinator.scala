package version0.core

import java.net.InetAddress

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}
import version0.setting.MySettings
import version0.zookeeper.ZkClient

import scala.actors.threadpool.{ExecutorService, Executors}
import scala.util.Random


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
  var driver: myDriver = _
  val threadPool:ExecutorService=Executors.newFixedThreadPool(100)
  val settings = new MySettings

  override def onStart(): Unit = {
    println("Coordinator is started")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case WriteCypher(cypher:String)=>{
      threadPool.execute(new CypherWriter(cypher, context, rpcEnv, settings.account, settings.password, settings.connectString, settings.sessionTimeout))
    }

    case ReadCypher(cypher:String)=>{
      // random choose a node
      val client = new ZkClient(settings.connectString, settings.sessionTimeout)
      client.getConnect()
      val hosts =client.getChildren
      val index = Random.nextInt(hosts.size())
      val randomIp = hosts.get(index)
      driver = new myDriver("bolt://" + randomIp + ":7687", "neo4j", "123123")
      driver.userReadCypher(cypher, context)
    }
  }
}
