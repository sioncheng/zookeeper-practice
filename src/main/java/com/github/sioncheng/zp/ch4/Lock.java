package com.github.sioncheng.zp.ch4;

import org.apache.zookeeper.*;

import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Lock {

    public static void main(String[] args) throws Exception {
        final ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 2000, new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println(Thread.currentThread().getName() + " " + event);
            }
        });

        final String monitor = "/zk/lock/monitor";

        for (int i = 0; i < 4; i++) {
            new Thread(new Runnable() {
                public void run() {
                    try {
                        String path = zooKeeper.create(monitor, "".getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        System.out.println("has lock");
                        System.out.println(path);
                    } catch (KeeperException.NodeExistsException ne) {
                        System.out.println("no lock");
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }).start();
        }


        TimeUnit.SECONDS.sleep(2);

        zooKeeper.close();
    }
}
