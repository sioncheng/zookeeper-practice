package com.github.sioncheng.zp.mw;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseTaskTest {

    @Test
    public void testParseTask() {
        String task = "1+2324";
        Pattern pattern = Pattern.compile("(\\d+)([\\+\\-\\*\\/])(\\d+)");
        Matcher matcher = pattern.matcher(task);

        Assert.assertNotNull(matcher);
        Assert.assertTrue(matcher.find());
        Assert.assertEquals(3, matcher.groupCount());
        Assert.assertEquals("1", matcher.group(1));
        Assert.assertEquals("+", matcher.group(2));
        Assert.assertEquals("2324", matcher.group(3));
    }
}
