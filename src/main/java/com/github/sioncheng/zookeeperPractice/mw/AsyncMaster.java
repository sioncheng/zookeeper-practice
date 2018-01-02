package com.github.sioncheng.zookeeperPractice.mw;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class AsyncMaster implements Watcher {

    AsyncMaster(String connectionString) {
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

    void checkMaster() {
        AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
            public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        checkMaster();
                        break;
                    case OK:
                        isLeader = (new String(bytes)).equalsIgnoreCase(serverId);
                        System.out.println(String.format("is leader %s in check master", isLeader));
                        break;
                    case NONODE:
                        runForMaster();
                        break;
                }
            }
        };

        zk.getData("/master", false, dataCallback, null);
    }

    void runForMaster()  {
        AsyncCallback.StringCallback stringCallback = new AsyncCallback.StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        checkMaster();
                        break;
                    case OK:
                        isLeader = true;
                        System.out.println(String.format("after creation %s", isLeader));
                        break;
                    default:
                        isLeader = false;
                        break;
                }
            }
        };

        zk.create("/master"
                , serverId.getBytes()
                , OPEN_ACL_UNSAFE
                , CreateMode.EPHEMERAL
                , stringCallback
                , null);
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        AsyncMaster m = new AsyncMaster(args[0]);
        m.startZk();

        m.runForMaster();

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
