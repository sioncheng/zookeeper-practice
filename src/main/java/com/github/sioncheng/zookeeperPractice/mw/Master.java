package com.github.sioncheng.zookeeperPractice.mw;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Master implements Watcher {

    Master(String connectionString) {
        this.connectionString = connectionString;
        this.serverId = Integer.toHexString((new Random().nextInt()));
        this.isLeader = false;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(this.connectionString, timeout, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    boolean checkMaster() throws InterruptedException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/master", false, stat);
                boolean leader = new String(data).equalsIgnoreCase(serverId);
                System.out.println(leader);
                return leader;
            } catch (KeeperException.NoNodeException ne) {
                return false;
            } catch (KeeperException.ConnectionLossException le) {
                System.out.println("connection lost exception");
            } catch (KeeperException ke) {
                System.out.println(String.format("%s %s", ke.code(), ke.getMessage()));
                return false;
            }
        }
    }

    void runForMaster() throws InterruptedException {
        while(true) {
            try {
                zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
            } catch (KeeperException.NodeExistsException ee) {
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException le) {
                System.out.println("connection lost exception");
            } catch (KeeperException ke) {
                isLeader = false;
                break;
            }
            if (checkMaster()) break;
        }
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Master m = new Master(args[0]);
        m.startZk();

        m.runForMaster();

        System.out.println(m.isLeader);

        while (true) {
            int ch = System.in.read();
            if (ch == (byte)'q') {
                break;
            }
        }

        m.stopZk();
    }

    private String connectionString;
    private ZooKeeper zk;
    private String serverId;
    private volatile boolean isLeader;

    private static final int timeout = 15000;
}
