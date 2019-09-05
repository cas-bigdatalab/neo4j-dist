package version0

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import version0.zookeeper.DistributeClient

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

object RpcClient {

  def main(args: Array[String]): Unit = {
    asyncCall()
  }

  def asyncCall() = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "hello-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    try{
//      client random choose a ip to send RPC request
      val client = new DistributeClient()
      client.getConnect()
      val hosts =client.getChildren
      val index = Random.nextInt(hosts.size())
      val randomIp = hosts.get(index)
      println("random ip is :" + randomIp)
      val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(randomIp, 6668), "server")

//      choose ReadCypher or WriteCypher

//      val future: Future[String] = endPointRef.ask[String](WriteCypher("CREATE(n:TEST {name:'qqq-ccc', age:1})", true))
      val future: Future[String] = endPointRef.ask[String](ReadCypher("match(n:TEST) return n.name as name, n.age as age"))

      future.onComplete {
        case scala.util.Success(value) => {
          println(s"Got the result: \n $value")
        }
        case scala.util.Failure(e) => {
          println(s"Got error: $e")
        }
      }
      val res = Await.result(future, Duration.apply("30s"))
      println("res: " + res)
    }catch {
      case e:Exception =>println(s"error: $e")
    }
  }
}
