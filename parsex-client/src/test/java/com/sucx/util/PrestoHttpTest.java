package com.sucx.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.sql.parser.ParsingException;
import com.sucx.util.HttpUtils;
import com.sucx.core.PrestoSqlParse;
import com.sucx.common.exceptions.SqlParseException;
import com.sucx.common.model.Result;
import com.sucx.core.SqlParseUtil;
import org.apache.http.message.BasicHeader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
                "_ga=GA1.2.1045647750.1571648344; 7ce0ff06556c05363a176b03dfdd5680=1160; a608ea7c4cbd1919ce039822a2e5d753=01160; cd1f6c4c522c03e21ad83ee2d7b0c515=%E8%8B%8F%E6%89%BF%E7%A5%A5%EF%BC%88%E8%8E%AB%E9%82%AA%EF%BC%89; e255ad9b8262a02d28bca48235a96357=1346; SSO_USER_TOKEN=p_19daf9e8b43332801f3d479b164cecfb"
        ));
    }

    @Test
    public void get() throws SqlParseException {

        String s = HttpUtils.doGet("https://prestonew-presto.bigdata-cn.xx-inc.top:7799/v1/query", headers);

        JSONArray array = JSONArray.parseArray(s);

        int size = array.size();
        System.out.println(size);

        PrestoSqlParse sqlParse = new PrestoSqlParse();
        for (int i = 0; i < size; i++) {
            JSONObject object = array.getJSONObject(i);
            String query = object.getString("query");
            System.out.println(query);

            Result parse = null;
            try {
                parse = sqlParse.parse(query);
            } catch (SqlParseException e) {
                if (e.getCause() instanceof ParsingException) {
                    System.out.println("sql解析异常:" + e.getMessage());
                } else {
                    throw new SqlParseException(e);
                }
            }
            SqlParseUtil.print(parse);
        }
    }

    public void test() {


        ArrayList<String> test = new ArrayList<>();


        List<String> collect = test.stream().map(col -> {
            return col + "1";
        }).collect(Collectors.toList());

        for (String s : collect) {
            System.out.println(s);
        }
    }

    @Test
    public void replace() {

        String text = "10.1.1 ";


        System.out.println(text.replaceAll("(.*)", ""));

    }

}
