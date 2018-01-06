package com.github.sioncheng.zp.mw;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author : cyq
 * @date : 03/01/2018 2:49 PM
 * Description:
 */
public class Client {

    public static void main(String[] args) throws IOException, InterruptedException {

        Client client = new Client(args[0]);
        client.start();

        StringBuilder sb = new StringBuilder();
        while (true) {
            char c = (char)System.in.read();
            if (c == 'q') {
                break;
            }

            if (c == '\r' || c == '\n') {
                parseAndCreateTask(client, sb.toString().trim());
            } else {
                sb.append(c);
            }
        }

        client.stop();
    }

    static void parseAndCreateTask(Client c, String s) {
        Task task = TaskUtil.parseTask(s);
        if (task == null) {
            logger.warn(String.format("can parse task %s", s));
            return;
        }
        String uuid = (UUID.randomUUID()).toString().replace("-","");
        task.setUuid(uuid);

        c.createTask(uuid, TaskUtil.serializeTask(task));
    }

    public Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void start() throws IOException {

        Watcher watcher = new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() != Event.KeeperState.SyncConnected) {
                    logger.error(String.format("cant connect to zk"));
                }
            }
        };

        zk = new ZooKeeper(this.hostPort, 5000, watcher);
    }

    void stop() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    void createTask(final String uuid, final byte[] data) {
        final String path = String.format("/tasks/%s", uuid);

        AsyncCallback.StringCallback stringCallback = new AsyncCallback.StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                KeeperException.Code code = KeeperException.Code.get(i);
                switch (code) {
                    case OK:
                        //
                        watchStatus(uuid);
                        break;
                    case CONNECTIONLOSS:
                        createTask(uuid, data);
                        break;
                    default:
                        logger.error(String.format("what happened ? %s", KeeperException.create(code, path)));
                        break;
                }
            }
        };

        zk.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, stringCallback, null);
    }

    void watchStatus(final String uuid) {
        final String statusPath = String.format("/status/%s", uuid);

        Watcher watcher = new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeCreated) {
                    getTaskResult(statusPath);
                }
            }
        };

        AsyncCallback.StatCallback statCallback = new AsyncCallback.StatCallback() {
            public void processResult(int i, String s, Object o, Stat stat) {
                KeeperException.Code code = KeeperException.Code.get(i);
                switch (code) {
                    case OK:
                        //
                        break;
                    case CONNECTIONLOSS:
                        Runnable runnable = new Runnable() {
                            public void run() {
                                watchStatus(uuid);
                            }
                        };

                        executor.execute(runnable);
                        break;
                    default:
                        logger.error(String.format("what happened ? %s", KeeperException.create(code, statusPath)));
                        break;
                }
            }
        };

        zk.exists(statusPath, watcher, statCallback, uuid);
    }

    void getTaskResult(final String statusPath) {

        AsyncCallback.DataCallback cb = new AsyncCallback.DataCallback() {
            public void processResult(int i, String s, Object o, byte[] bytes, final Stat stat) {
                KeeperException.Code code = KeeperException.Code.get(i);
                switch (code) {
                    case OK:
                        //
                        TaskResult taskResult = TaskUtil.deserializeTaskResult(bytes);
                        logger.info(String.format("task result %s", taskResult));
                        break;
                    case CONNECTIONLOSS:
                        Runnable runnable = new Runnable() {
                            public void run() {
                                getTaskResult(statusPath);
                            }
                        };

                        executor.execute(runnable);
                    default:
                        logger.error(String.format("what happened ? %s", KeeperException.create(code, statusPath)));
                        break;
                }
            }
        };

        zk.getData(statusPath,false, cb,null);
    }

    private String hostPort;
    private ZooKeeper zk;

    private static Logger logger = Logger.getLogger(Client.class);

    private static Executor executor = Executors.newSingleThreadExecutor();
}
