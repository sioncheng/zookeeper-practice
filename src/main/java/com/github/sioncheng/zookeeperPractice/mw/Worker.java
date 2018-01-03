package com.github.sioncheng.zookeeperPractice.mw;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author : cyq
 * @date : 03/01/2018 2:49 PM
 * Description:
 */
public class Worker {

    public static void main(String[] args) throws Exception {
        Worker worker = new Worker(args[0]);
        worker.start();

        while(true) {
            int q = System.in.read();
            if (q == (int)'q') {
                break;
            }
        }

        worker.stop();
    }

    Worker(String hostPort) {
        this.hostPort = hostPort;
        this.workerId = UUID.randomUUID().toString().replace("-","");
        this.processingTasks = new ConcurrentHashMap<String, String>();
    }

    void start() throws IOException {
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent event) {
                prepareRegister();
            }
        };

        zk = new ZooKeeper(this.hostPort, 5000, watcher);
    }

    void stop() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    void prepareRegister() {
        AsyncCallback.StringCallback cb = new AsyncCallback.StringCallback() {
            public void processResult(int rc, String path, Object ctx, String name) {
                KeeperException.Code code = KeeperException.Code.get(rc);
                switch (code) {
                    case OK:
                        //
                        logger.info(String.format("prepared as worker %s", workerId));
                        register();
                        break;
                    case CONNECTIONLOSS:
                        register();
                }
            }
        };

        zk.create(String.format("/assign/worker-%s", workerId)
                , "".getBytes()
                , OPEN_ACL_UNSAFE
                , CreateMode.PERSISTENT
                , cb
                , null);
    }

    void register() {

        AsyncCallback.StringCallback cb = new AsyncCallback.StringCallback() {
            public void processResult(int rc, String path, Object ctx, String name) {
                KeeperException.Code code = KeeperException.Code.get(rc);
                switch (code) {
                    case OK:
                        //
                        logger.info(String.format("registered as worker %s", workerId));
                        watchAssign();
                        break;
                    case CONNECTIONLOSS:
                        register();
                }
            }
        };

        zk.create(String.format("/workers/worker-%s", workerId)
                , workerId.getBytes()
                , OPEN_ACL_UNSAFE
                , CreateMode.EPHEMERAL
                , cb
                , null);
    }

    void watchAssign() {
        final String watchPath = String.format("/assign/worker-%s", workerId);
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    watchAssign();
                }
            }
        };
        AsyncCallback.ChildrenCallback childrenCallback = new AsyncCallback.ChildrenCallback() {
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                KeeperException.Code code = KeeperException.Code.get(rc);
                switch (code) {
                    case OK:
                        //
                        for(String child: children) {
                            if (processingTasks.contains(child)) {
                                continue;
                            }

                            executeTask(child);
                        }
                        break;
                    case CONNECTIONLOSS:
                        watchAssign();
                        break;
                    default:
                        logger.warn(String.format("whats wrong ? %s", KeeperException.create(code, path)));
                }
            }
        };
        zk.getChildren(watchPath,watcher,childrenCallback,null);
    }

    void executeTask(String taskId) {
        logger.info(String.format("execute task %s", taskId));
    }

    private String hostPort;
    private ZooKeeper zk;
    private String workerId;
    private ConcurrentHashMap<String, String> processingTasks;

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(Worker.class);
}
