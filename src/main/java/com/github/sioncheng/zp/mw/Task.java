package com.github.sioncheng.zp.mw;

/**
 * @author : cyq
 * @date : 03/01/2018 2:52 PM
 * Description:
 */
public class Task {

    public Task(){
        stringExpress = null;
    }

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

    @Override
    public String toString() {
        if (stringExpress ==  null) {
            stringExpress = String.format("%d %s %d", this.leftNumber, this.operation, this.rightNumber);
        }

        return stringExpress;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }

        if (obj instanceof Task == false) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        Task that = (Task)obj;

        return this.hashCode() == that.hashCode();
    }

    private String uuid;
    private int leftNumber;
    private String operation;
    private int rightNumber;

    private String stringExpress;
}
