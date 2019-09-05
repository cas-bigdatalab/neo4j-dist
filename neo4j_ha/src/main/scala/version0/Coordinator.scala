package version0

import java.net.InetAddress

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv, RpcEnvClientConfig, RpcEnvServerConfig, RpcTimeout}
import version0.zookeeper.DistributeClient

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
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

  override def onStart(): Unit = {
    println("Coordinator is started")
  }

  override def onConnected(remoteAddress: RpcAddress): Unit = {
    println("onConnected...")
  }

  override def onStop(): Unit = {
    println("Coordinator is stopped")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case WriteCypher(cypher: String, isDistribute: Boolean) => {
      //if comes from client, send cypher to cluster nodes
      if (isDistribute) {
        distributeCypher(cypher, context)
      } else {
        //if not from client, run task.
        runDriverNotCommit(cypher, context)
      }
    }
    case CommitCypher(flag: Boolean) => {
      if (flag) {
        driver.driverSuccess(context)
      } else {
        driver.driverFailed(context)
      }
    }
    case ReadCypher(cypher:String)=>{
      // random choose a node
      val client = new DistributeClient()
      client.getConnect()
      val hosts =client.getChildren
      val index = Random.nextInt(hosts.size())
      val randomIp = hosts.get(index)
      driver = new myDriver("bolt://" + randomIp + ":7687", "neo4j", "123123")
      driver.userReadCypher(cypher, context)
    }
  }

  def distributeCypher(cypher: String, context: RpcCallContext): Unit = {
    //connect to other rpc and receive the result of writing

    //get hosts from zookeeper
    val client = new DistributeClient()
    client.getConnect()
    val hosts =client.getChildren

    val host = InetAddress.getLocalHost.getHostAddress
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, host)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    // distribute RPC，parallel execution
    var count = 0
    var futureArray = ArrayBuffer[Future[String]]()
    for (i <- 0.until(hosts.size())) {
      val ip = hosts.get(i)
      try {
        val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(ip, 6668), "server")
        val future: Future[String] = endPointRef.ask[String](WriteCypher(cypher, false))
        println(ip + " reachable")
        future.onComplete {
          case scala.util.Success(value) => {
            println("success~~: " + value)
            count += 1
          }
          case scala.util.Failure(e) => {
            println(s"Got error: $e")
          }
        }
        futureArray += future
      } catch {
        case e: Exception => {
          println(ip + " unreachable: " + e.getMessage)
        }
      }
    }
    futureArray.par.foreach(f => Await.ready(f, Duration.apply("30s")))
    println(count + " nodes are successfully written")

    if (count == hosts.size()) {
      // commit session success to database using RPC
      val rpcConf = new RpcConf()
      val config = RpcEnvClientConfig(rpcConf, "hello-server-again")
      val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

      val cFutureArray = ArrayBuffer[Future[String]]()

      for (i <- 0.until(hosts.size())) {
        val ip = hosts.get(i)
        val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(ip, 6668), "server")
        val future: Future[String] = endPointRef.ask[String](CommitCypher(true))
        future.onComplete {
          case scala.util.Success(value) => {
            println("success: " + value + System.currentTimeMillis())
          }
          case scala.util.Failure(e) => {
            println(s"Got error: $e")
          }
        }
        cFutureArray += future
      }
      cFutureArray.par.foreach(f => Await.ready(f, Duration.apply("30s")))
      context.reply("Write Success!!！")
    }
    else {
      //commit session failed to database using RPC
      val rpcConf = new RpcConf()
      val config = RpcEnvClientConfig(rpcConf, "hello-server-again")
      val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

      var fFutureArray = ArrayBuffer[Future[String]]()

      for (i <- 0.until(hosts.size())) {
        val ip = hosts.get(i)
        val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(ip, 6668), "server")
        val future: Future[String] = endPointRef.ask[String](CommitCypher(false))
        future.onComplete {
          case scala.util.Success(value) => {
            println("success: " + value + System.currentTimeMillis())
          }
          case scala.util.Failure(e) => {
            println(s"Got error: $e")
          }
        }
        fFutureArray += future
      }
      fFutureArray.par.foreach(f =>Await.ready(f, Duration.apply("30s")))
      context.reply("Write Failed！")
    }
  }


  def runDriverNotCommit(cypher: String, context: RpcCallContext): Unit = {
    println("I'm running the writing task!!!")

    // write neo4j database, if success, reply 1, else reply 0
    driver = new myDriver("bolt://localhost:7687", "neo4j", "123123")
    val res = driver.userWriteCypher(cypher)
    if (res == "success") {
      context.reply("1")
    } else {
      context.reply("0")
    }
    println(res)
  }
}
