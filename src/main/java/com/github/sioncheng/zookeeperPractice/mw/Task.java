package com.github.sioncheng.zookeeperPractice.mw;

/**
 * @author : cyq
 * @date : 03/01/2018 2:52 PM
 * Description:
 */
public class Task {

    public Task(){}

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public int getLeftNumber() {
        return leftNumber;
    }

    public void setLeftNumber(int leftNumber) {
        this.leftNumber = leftNumber;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public int getRightNumber() {
        return rightNumber;
    }

    public void setRightNumber(int rightNumber) {
        this.rightNumber = rightNumber;
    }

    private String uuid;
    private int leftNumber;
    private String operation;
    private int rightNumber;
}
