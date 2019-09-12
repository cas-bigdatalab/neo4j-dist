package version0.zookeeper;

import org.apache.zookeeper.*;
import version0.setting.MySettings;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

public class ZkServer {
    // your zookeeper connection string
    private String connectString;
    private int sessionTimeout;

    private ZooKeeper zk = null;
    private String parentNode = "/servers";
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public ZkServer(String connectString, int sessionTimeout) {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
    }

    // create zk
    public void getConnect(){
        try {
            zk = new ZooKeeper(connectString, sessionTimeout, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    countDownLatch.countDown();
                }
            });
            countDownLatch.await();
            System.out.println("zookeeper connection success");
        }catch (Exception e){
            System.out.println("on getConnection error: " + e.getMessage());
        }
    }


    // register server
    public void registerServer(String hostname) throws Exception {
        try {
            if (zk.exists(parentNode, false) == null) {
                zk.create(parentNode, "servers".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String create = zk.create(parentNode + "/server", hostname.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(hostname + " is online " + create);

        } catch (Exception e) {
            System.out.println("registerServer..." + e.getMessage());
        }
    }

    // make sure this node is online
    public void business() throws Exception {
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        MySettings settings = new MySettings();
        ZkServer server = new ZkServer(settings.connectString(),settings.sessionTimeout());
        server.getConnect();
        server.registerServer(InetAddress.getLocalHost().getHostAddress());
        server.business();
    }
}
