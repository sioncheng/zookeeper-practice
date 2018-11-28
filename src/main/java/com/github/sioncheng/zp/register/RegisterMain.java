package com.github.sioncheng.zp.register;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class RegisterMain {

    public static void main(String[] args) throws IOException {

        zk = new ZooKeeper("localhost:2181", 5000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getPath());
                System.out.println(watchedEvent.getState().name());
                System.out.println(watchedEvent.getType().name());
            }
        });


        int r = System.in.read();
        while (r != (int)'q') {
            r = System.in.read();
        }

        System.out.println("bye");
    }

    private static volatile ZooKeeper zk;
}
