package version1.test

import java.net.InetAddress

import scala.concurrent.ExecutionContext.Implicits.global
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import version1.core.{ReadCypher, WriteCypher}
import version1.setting.MySettings

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class client(cypher:String, time:Long) extends Runnable{
  val setting = new MySettings
  val host = InetAddress.getLocalHost.getHostAddress
  val rpcConf = new RpcConf()
  val config = RpcEnvClientConfig(rpcConf, host)
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
  val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("192.168.49.10", 6668), "server")

  override def run(): Unit = {
    val future: Future[String] = endPointRef.ask[String](ReadCypher(cypher))
    future.onComplete {
      case scala.util.Success(value) => {
        println(s"Got the result: \n $value")
        println("Total Tiem: " + (System.currentTimeMillis() - time).toString)
        rpcEnv.stop(endPointRef)
        rpcEnv.shutdown()
      }
      case scala.util.Failure(e) => {
        println(s"Got error: $e")
        rpcEnv.stop(endPointRef)
        rpcEnv.shutdown()
      }
    }
    Await.ready(future, Duration.apply("Inf"))
  }
}
