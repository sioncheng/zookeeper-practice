package com.github.sioncheng.zp.mw;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class AsyncMaster implements Watcher {


    public static void main(String[] args) throws IOException, InterruptedException {
        AsyncMaster m = new AsyncMaster(args[0]);
        m.startZk();

        m.bootstrap();

        m.runForMaster();

        while (true) {
            int ch = System.in.read();
            if (ch == (byte) 'q') {
                break;
            }
        }

        m.stopZk();
    }

    AsyncMaster(String connectionString) {
        this.connectionString = connectionString;
        this.serverId = Integer.toHexString((new Random().nextInt()));
        this.isLeader = false;
        this.executor = Executors.newCachedThreadPool();
        this.workNumber = 0;
        this.workers = new ConcurrentHashMap<Integer, String>();
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
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
                KeeperException.Code code = KeeperException.Code.get(i);
                switch (KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        checkMaster();
                        break;
                    case OK:
                        isLeader = (new String(bytes)).equalsIgnoreCase(serverId);
                        logger.info(String.format("is leader %s in check master", isLeader));
                        if (isLeader == false) {
                            becameSlaveMaster();
                        } else {
                            becamePrimaryMaster();
                        }
                        break;
                    case NONODE:
                        runForMaster();
                        break;
                    default:
                        logger.warn(String.format("what happened ? %s", KeeperException.create(code, "/master")));
                }
            }
        };

        zk.getData("/master", false, dataCallback, null);
    }

    void runForMaster() {
        AsyncCallback.StringCallback stringCallback = new AsyncCallback.StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                KeeperException.Code code = KeeperException.Code.get(i);
                switch (code) {
                    case CONNECTIONLOSS:
                        checkMaster();
                        break;
                    case OK:
                        isLeader = true;
                        logger.info(String.format("after creation %s", isLeader));
                        becamePrimaryMaster();
                        break;
                    default:
                        isLeader = false;
                        logger.info(String.format("became slave master %s", code.toString()));
                        becameSlaveMaster();
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

    void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
        createParent("/goneWorkers", new byte[0]);
    }

    private void createParent(final String path, final byte[] data) {
        AsyncCallback.StringCallback stringCallback = new AsyncCallback.StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                KeeperException.Code code = KeeperException.Code.get(i);
                switch (KeeperException.Code.get(i)) {
                    case OK:
                        logger.info(String.format("%s created", path));
                        break;
                    case NODEEXISTS:
                        logger.info(String.format("%s already existed", path));
                        break;
                    case CONNECTIONLOSS:
                        createParent(path, data);
                        break;
                    default:
                        logger.error(String.format("%s happened", KeeperException.create(code, path)));
                        break;
                }
            }
        };

        zk.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, stringCallback, data);
    }

    void becameSlaveMaster() {
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                    runForMaster();
                }
            }
        };

        AsyncCallback.StatCallback statCallback = new AsyncCallback.StatCallback() {
            public void processResult(int i, String s, Object o, Stat stat) {
                KeeperException.Code code = KeeperException.Code.get(i);
                switch (code) {
                    case OK:
                        logger.info("became slave master");
                        break;
                    case CONNECTIONLOSS:
                        becameSlaveMaster();
                        break;
                    default:
                        logger.warn(String.format("what happened %s", code.toString()));
                        break;
                }
            }
        };

        zk.exists("/master", watcher, statCallback, null);
    }

    void becamePrimaryMaster() {
        watchWorkers();
        //watchTasks();
    }

    void watchWorkers() {
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (isLeader == false) {
                    logger.warn("i am not a leader now?");
                    return;
                }

                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    watchWorkers();
                }
            }
        };

        AsyncCallback.ChildrenCallback childrenCallback = new AsyncCallback.ChildrenCallback() {
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                KeeperException.Code code = KeeperException.Code.get(rc);
                switch (code) {
                    case CONNECTIONLOSS:
                        watchWorkers();
                        break;
                    case OK:
                        //
                        List<String> goneChildren = new ArrayList<String>();
                        for (Map.Entry<Integer, String> kv : workers.entrySet()) {
                            if (children.contains(kv.getValue()) == false) {
                                goneChildren.add(kv.getValue());
                            }
                        }

                        workers.clear();
                        workNumber = 0;
                        int i = 0;
                        for (String child : children) {
                            workers.put(i, child);
                            i++;
                        }

                        if (workers.size() > 0) {
                            watchTasks();
                        } else {
                            logger.warn("there is no worker now.");

                            Runnable runnable = new Runnable() {
                                public void run() {

                                    try {
                                        Thread.sleep(1000);
                                    } catch (InterruptedException ie) {
                                        logger.warn(ie.toString());
                                    }
                                    watchWorkers();
                                }
                            };

                            executor.execute(runnable);
                        }

                        for (String child : goneChildren) {
                            checkUncompletedTasks(child, workers.size() > 0);
                        }

                        break;
                    default:
                        logger.error(String.format("what happened %s", KeeperException.create(code, "/tasks")));
                }
            }
        };

        zk.getChildren("/workers", watcher, childrenCallback, null);
    }

    void watchTasks() {
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (isLeader == false) {
                    logger.warn("i am not a leader now?");
                    return;
                }

                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    watchTasks();
                }
            }
        };

        AsyncCallback.ChildrenCallback childrenCallback = new AsyncCallback.ChildrenCallback() {
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                KeeperException.Code code = KeeperException.Code.get(rc);
                switch (code) {
                    case CONNECTIONLOSS:
                        becamePrimaryMaster();
                        break;
                    case OK:
                        //
                        for (String child : children) {
                            logger.info(String.format("going to assign task %s", child));
                            assignTask(child);
                        }
                        break;
                    default:
                        logger.error(String.format("what happened %s", KeeperException.create(code, "/tasks")));
                }
            }
        };

        zk.getChildren("/tasks", watcher, childrenCallback, null);
    }

    void assignTask(final String task) {

        final byte[] data = task.getBytes();

        int workerI = workNumber;
        workNumber++;
        if (workerI > workers.size()) {
            workerI = 0;
            workNumber = 0;
        }

        final String worker = workers.get(workerI);

        final AsyncCallback.StringCallback stringCallback = new AsyncCallback.StringCallback() {
            public void processResult(int rc2, String path2, Object ctx2, String name2) {
                KeeperException.Code code2 = KeeperException.Code.get(rc2);
                switch (code2) {
                    case OK:
                        logger.info(String.format("assigned task %s", path2));
                        break;
                    case CONNECTIONLOSS:
                        assignTask(task);
                        break;
                    default:
                        logger.error(String.format("what happened %s", KeeperException.create(code2, path2)));
                        break;
                }
            }
        };

        zk.create(String.format("/assign/%s/%s", worker, task)
                , data
                , OPEN_ACL_UNSAFE
                , CreateMode.PERSISTENT, stringCallback, data);
    }

    void checkUncompletedTasks(final String goneWorker, final boolean hasNewWorkers) {
        if (hasNewWorkers) {
            final String assignPath = String.format("/assign/%s", goneWorker);

            Watcher getChildrenWatcher = new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    logger.info(watchedEvent.getPath());
                }
            };

            AsyncCallback.ChildrenCallback getChildrenCallback = new AsyncCallback.ChildrenCallback() {
                public void processResult(int i, String s, Object o, List<String> list) {
                    KeeperException.Code code = KeeperException.Code.get(i);
                    switch (code) {
                        case OK:
                            //
                            for (String taskId :
                                    list) {
                                assignTask(taskId);
                            }
                            break;
                        case CONNECTIONLOSS:
                            checkUncompletedTasks(goneWorker, hasNewWorkers);
                            break;
                        default:
                            logger.error(String.format("what happened %s", KeeperException.create(code, assignPath)));
                            break;
                    }
                }
            };

            zk.getChildren(assignPath, getChildrenWatcher, getChildrenCallback, null);
        } else {
            AsyncCallback.StringCallback stringCallback = new AsyncCallback.StringCallback() {
                public void processResult(int i, String s, Object o, String s1) {
                    KeeperException.Code code = KeeperException.Code.get(i);
                    switch (code) {
                        case OK:
                            //
                            break;
                        case CONNECTIONLOSS:
                            checkUncompletedTasks(goneWorker, hasNewWorkers);
                            break;
                        default:
                            logger.error(String.format("what happened %s", KeeperException.create(code, s)));
                            break;
                    }
                }
            };
            zk.create(String.format("/goneWorkers/%s", goneWorker)
                    , "".getBytes()
                    , OPEN_ACL_UNSAFE
                    , CreateMode.PERSISTENT
                    , stringCallback
                    , null);
        }
    }

    private String connectionString;
    private ZooKeeper zk;
    private String serverId;
    private Executor executor;
    private volatile int workNumber;
    private ConcurrentHashMap<Integer, String> workers;
    private volatile boolean isLeader;

    private static final int timeout = 15000;

    private static Logger logger = Logger.getLogger(AsyncMaster.class);
}
