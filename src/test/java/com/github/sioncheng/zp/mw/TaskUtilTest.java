package com.github.sioncheng.zp.mw;

import org.junit.Assert;
import org.junit.Test;

public class TaskUtilTest {

    @Test
    public void testParseTask() {
        Task task = TaskUtil.parseTask(" 1 +   131231 ");
        Assert.assertNotNull(task);
        Assert.assertEquals(1, task.getLeftNumber());
        Assert.assertEquals("+", task.getOperation());
        Assert.assertEquals(131231, task.getRightNumber());
    }
}
