package version0

import java.net.InetAddress

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import version0.core.WriteCypher
import version0.setting.MySettings
import version0.zookeeper.ZkClient

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

object RpcClient {

  def main(args: Array[String]): Unit = {
    val setting = new MySettings
    val host = InetAddress.getLocalHost.getHostAddress
    asyncCall(host, setting.connectString, setting.sessionTimeout)
  }

  def asyncCall(host:String, connectStr:String, sessionTimeout:Int) = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, host)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    try{
//      client random choose a ip to send RPC request
      val client = new ZkClient(connectStr, sessionTimeout)
      client.getConnect()
      val hosts =client.getChildren
      val index = Random.nextInt(hosts.size())
      val randomIp = hosts.get(index)
      println(randomIp)
      val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(randomIp, 6668), "server")

//      choose ReadCypher or WriteCypher

      val future: Future[String] = endPointRef.ask[String](WriteCypher("CREATE(n:TEST {name:'ccc', age:1})"))
//      val future: Future[String] = endPointRef.ask[String](ReadCypher("match(n:TEST) return n.name as name, n.age as age"))

      future.onComplete {
        case scala.util.Success(value) => {
          println(s"Got the result: \n $value")
        }
        case scala.util.Failure(e) => {
          println(s"Got error: $e")
        }
      }
      Await.ready(future, Duration.apply("30s"))
    }catch {
      case e:Exception =>println(s"error: $e")
    }
  }
}
