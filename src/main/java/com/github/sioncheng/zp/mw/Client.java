package com.github.sioncheng.zp.mw;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;

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
        Task task = TaskSerializer.parseTask(s);
        if (task == null) {
            logger.warn(String.format("can parse task %s", s));
            return;
        }

        c.createTask(TaskSerializer.serializeTask(task));
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

    void sendTask(Task task) {
        byte[] data = TaskSerializer.serializeTask(task);
        createTask(data);
    }

    void createTask(final byte[] data) {
        final String path = "/tasks/t";

        AsyncCallback.StringCallback stringCallback = new AsyncCallback.StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                KeeperException.Code code = KeeperException.Code.get(i);
                switch (code) {
                    case OK:
                        //
                        break;
                    case CONNECTIONLOSS:
                        createTask(data);
                        break;
                    default:
                        logger.error(String.format("what happened ? %s", KeeperException.create(code, path)));
                }
            }
        };

        zk.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, stringCallback, null);
    }

    private String hostPort;
    private ZooKeeper zk;

    private static Logger logger = Logger.getLogger(Client.class);
}
