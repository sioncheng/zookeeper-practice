package com.github.sioncheng.zp.mw;

public class TaskResult {

    public TaskResult() {

    }

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }

    private Task task;

    private int result;
}
