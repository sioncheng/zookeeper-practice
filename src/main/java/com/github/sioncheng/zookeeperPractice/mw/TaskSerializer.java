package com.github.sioncheng.zookeeperPractice.mw;

import com.alibaba.fastjson.JSON;

/**
 * @author : cyq
 * @date : 03/01/2018 2:53 PM
 * Description:
 */
public class TaskSerializer {

    public static byte[] serializeTask(Task task) {
        return JSON.toJSONString(task).getBytes();
    }

    public static Task deserializeTask(byte[] data) {
        return JSON.parseObject(new String(data), Task.class);
    }
}
