package com.tuya.util;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * desc:
 *
 * @author scx
 * @create 2020/03/02
 */
public class HeraTest {


    @Test
    public void buildJson() throws IOException {
        //创建测试文件
        File file = new File("/Users/scx/Desktop/reportLog.txt");
        if (!file.exists()) {
            if (file.createNewFile()) {
                throw new IOException("新建文件失败:" + file.getAbsolutePath());
            }
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        // 100W的设备数量
        int devSize = 10000 * 100;
        // 上报类型
        String[] typeArr = {"OFFLINE", "ONLINE", "RESET", "ACTIVE"};
        // 10天的日期
        String[] dateArr =
                {"2020-02-01", "2020-02-02", "2020-02-03", "2020-02-04", "2020-02-05",
                        "2020-02-06", "2020-02-07", "2020-02-08", "2020-02-09", "2020-02-10"};
        Random random = new Random(99999);
        String type;
        for (String date : dateArr) {
            int activeCount = 0;
            for (int i = 1; i <= devSize; i++) {
                JSONObject json = new JSONObject();
                type = typeArr[random.nextInt(typeArr.length)];
                if ("ONLINE".equals(type) || "ACTIVE".equals(type)) {
                    activeCount++;
                }
                json.put("id", i);
                //随机赋予一种上报类型
                json.put("type", type);
                json.put("date", date);
                writer.write(json.toJSONString());
                writer.newLine();
            }
            System.out.println(String.format("日志:%s,活跃数:%d", date, activeCount));
        }
        writer.flush();
    }

}
