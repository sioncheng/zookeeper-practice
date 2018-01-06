package com.github.sioncheng.zp.mw;

import com.alibaba.fastjson.JSON;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author : cyq
 * @date : 03/01/2018 2:53 PM
 * Description:
 */
public class TaskUtil {

    public static byte[] serializeTask(Task task) {
        return JSON.toJSONString(task).getBytes();
    }

    public static Task deserializeTask(byte[] data) {
        return JSON.parseObject(new String(data), Task.class);
    }

    public static Task parseTask(String taskExpress) {
        Matcher matcher = pattern.matcher(taskExpress.replace(" ",""));
        if (matcher.find() == false) {
            return null;
        }

        Task task = new Task();
        task.setLeftNumber(Integer.parseInt(matcher.group(1)));
        task.setOperation(matcher.group(2));
        task.setRightNumber(Integer.parseInt(matcher.group(3)));

        return task;
    }

    public static int executeTask(Task task) {
        switch (task.getOperation().charAt(0)) {
            case '+':
                return task.getLeftNumber() + task.getRightNumber();
            case '-':
                return task.getLeftNumber() - task.getRightNumber();
            case '*':
                return task.getLeftNumber() * task.getRightNumber();
            case '/':
                return task.getLeftNumber() / task.getRightNumber();
        }

        return 0;
    }

    public static byte[] serializeTaskResult(TaskResult taskResult) {
        return JSON.toJSONString(taskResult).getBytes();
    }

    public static TaskResult deserializeTaskResult(byte[] data) {
        return JSON.parseObject(new String(data), TaskResult.class);
    }

    private static final Pattern pattern = Pattern.compile("(\\d+)([\\+\\-\\*\\/])(\\d+)");
}
