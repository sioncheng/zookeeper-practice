package com.github.sioncheng.zp.ch3;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

public class HelloZookeeper {

    public static void main(String[] args) throws IOException  {
        final String host = "localhost:2181";
        final String path = "/";

        ZooKeeper zooKeeper = new ZooKeeper(host, 2000, new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });

        if (zooKeeper == null) {
            return;
        }

        try {
            List<String> children = zooKeeper.getChildren(path, false);
            for (String child :
                    children) {
                System.out.println(child);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            saveClose(zooKeeper);
        }
    }

    private static void saveClose(ZooKeeper zooKeeper) {
        try {
            zooKeeper.close();
        } catch (Exception e) {}
    }
}
