package version0.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributeClient {
    private static String connectString = "192.168.49.10:2181,192.168.49.11:2181,192.168.49.12:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public void getConnect() throws IOException {
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
