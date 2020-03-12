package com.tuya.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tuya.core.PrestoSqlParse;
import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.TableInfo;
import com.tuya.core.util.SqlParseUtil;
import org.apache.http.message.BasicHeader;
import org.junit.Test;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * desc:
 *
 * @author scx
 * @create 2020/03/10
 */
public class PrestoHttpTest {
    ArrayList<BasicHeader> headers = new ArrayList<>();

    {
        headers.add(new BasicHeader("cookie",
                "_ga=GA1.2.1045647750.1571648344; 7ce0ff06556c05363a176b03dfdd5680=1160; a608ea7c4cbd1919ce039822a2e5d753=01160; cd1f6c4c522c03e21ad83ee2d7b0c515=%E8%8B%8F%E6%89%BF%E7%A5%A5%EF%BC%88%E8%8E%AB%E9%82%AA%EF%BC%89; e255ad9b8262a02d28bca48235a96357=1346; SSO_USER_TOKEN=p_586e8567ee97e69661a1238c0efe6d56"));
    }

    @Test
    public void get() throws SqlParseException {

        String s = HttpUtils.doGet("https://prestonew-presto.bigdata-cn.tuya-inc.top:7799/v1/query", headers);


        JSONArray array = JSONArray.parseArray(s);

        int size = array.size();
        System.out.println(size);

        PrestoSqlParse sqlParse = new PrestoSqlParse();
        for (int i = 0; i < size; i++) {
            JSONObject object = array.getJSONObject(i);

            String query = object.getString("query");
            System.out.println(query);
            Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse = sqlParse.parse(query);
            SqlParseUtil.print(parse);
        }
    }

}
