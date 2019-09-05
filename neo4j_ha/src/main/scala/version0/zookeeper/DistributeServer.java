package version0.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.net.InetAddress;

public class DistributeServer {
    // your zookeeper connection string
    private static String connectString = "192.168.49.10:2181,192.168.49.11:2181,192.168.49.12:2181";

    private static int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    // create zk
    public void getConnect() throws IOException {

        zk = new ZooKeeper(connectString, sessionTimeout, event -> {

        });
    }

    // register server
    public void registerServer(String hostname) throws Exception{
        if (zk.exists(parentNode, false) == null){
            zk.create(parentNode, "servers".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String create = zk.create(parentNode + "/server", hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname +" is online "+ create);
    }

    // make sure this node is online
    public void business() throws Exception{
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        DistributeServer server = new DistributeServer();
        server.getConnect();
        server.registerServer(InetAddress.getLocalHost().getHostAddress());
        server.business();
    }
}
