package com.sucx.common.util;


import com.sucx.common.Constants;

/**
 * desc:
 *
 * @author scx
 * @create 2020/03/12
 */
public class StringUtils {


    public static Pair<String, String> getPointPair(String content) {
        return getPair(Constants.POINT, content, false);
    }

    public static Pair<String, String> getLastPointPair(String content) {
        return getPair(Constants.POINT, content, true);
    }


    private static Pair<String, String> getPair(String split, String content, boolean dir) {
        int index;
        if (dir) {
            index = content.lastIndexOf(Constants.POINT);
        } else {
            index = content.indexOf(Constants.POINT);
        }
        if (index == -1) {
            throw new RuntimeException("not contain . character:" + content);
        }
        String left = content.substring(0, index);
        String right = content.substring(index + 1);
        return Pair.of(left, right);
    }
}
