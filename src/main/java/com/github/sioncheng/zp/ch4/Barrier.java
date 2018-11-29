package com.github.sioncheng.zp.ch4;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Barrier {

    public static void main(String[] args) throws Exception {

        final int barrierCount = 4;
        final String root = "/zk/barrier";

        final ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 2000, new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println(Thread.currentThread().getName() + " " + event);
            }
        });

        for (int i = 0 ; i < barrierCount; i++) {
            new Thread(new WorkerWithBarrier(zooKeeper, barrierCount, root, i)).start();
        }

        TimeUnit.SECONDS.sleep(4);

        zooKeeper.close();
    }

    static class WorkerWithBarrier implements Runnable {
        private final ZooKeeper zooKeeper;
        private final int barrierCount;
        private final String root;
        private final int number;

        public WorkerWithBarrier(ZooKeeper zooKeeper, int barrierCount, String root, int number) throws IOException {
            this.zooKeeper = zooKeeper;
            this.barrierCount = barrierCount;
            this.root = root;
            this.number = number;
        }

        public void run() {
            try {
                if (zooKeeper.exists(root, false) == null) {
                    try {
                        zooKeeper.create(root, "".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException ne) {

                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }

            try {
                String path = zooKeeper.create(root + "/node-" + number, "".getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                System.out.println("created " + path);
            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }

            await();
        }

        private void await() {
            try {
                List<String> children = zooKeeper.getChildren(root, new Watcher() {
                    public void process(WatchedEvent event) {
                        System.out.println(Thread.currentThread().getName() + " " + "re-watch");
                        await();
                    }
                });

                for (String child :
                        children) {
                    System.out.println(child);
                }

                if (children.size() == barrierCount) {
                    System.out.println("ready go " + number);
                } else {
                    TimeUnit.SECONDS.sleep(3);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }
        }

    }
}
