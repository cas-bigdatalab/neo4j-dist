package main.scala.version1.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkClient {
    private  String connectString;
    private  int sessionTimeout;

    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public ZkClient(String connectString, int sessionTimeout) {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
    }

    public void getConnect(){
        try{
            zk = new ZooKeeper(connectString, sessionTimeout, event -> {
                if(event.getState()== Watcher.Event.KeeperState.SyncConnected){
                    countDownLatch.countDown();
                }
            });
            countDownLatch.await();
            System.out.println("zookeeper connection success");
        }catch(Exception e){
            System.out.println("getConnect..." + e.getMessage());
        }
    }

    public ArrayList<String> getChildren() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(parentNode, true);
        ArrayList<String> hosts = new ArrayList<>();
        for (String child: children) {
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            hosts.add(new String(data));
        }
        zk.close();
        return hosts;
    }

}
