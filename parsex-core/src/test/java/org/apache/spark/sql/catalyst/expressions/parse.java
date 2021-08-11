package org.apache.spark.sql.catalyst.expressions;

import com.sucx.core.SparkSQLParse;
import com.sucx.core.SqlParse;
import com.sucx.common.exceptions.SqlParseException;
import com.sucx.common.model.Result;
import com.sucx.core.SqlParseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/24
 */
public class parse {

    String sql = "";

    @Test
    public void parse() throws SqlParseException {

        SqlParse sqlParse = new SparkSQLParse();

        Result tuple3 = sqlParse.parse(sql);

        SqlParseUtil.print(tuple3);

    }


    @Test
    public void splitSql() throws IOException {
        //  deal(sql);

        readFile("/Users/scx/Desktop/test.hive");
    }

    public void readFile(String fileName) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(fileName);
        FileSystem fs;
        if (!path.toUri().isAbsolute()) {
            fs = FileSystem.getLocal(conf);
            path = fs.makeQualified(path);
        } else {
            fs = FileSystem.get(path.toUri(), conf);
        }
        BufferedReader bufferReader = null;
        int rc = 0;
        try {
            bufferReader = new BufferedReader(new InputStreamReader(fs.open(path)));
            processReader(bufferReader);
        } finally {
            IOUtils.closeStream(bufferReader);
        }
    }

    public void processReader(BufferedReader r) throws IOException {
        String line;
        StringBuilder qsb = new StringBuilder();

        while ((line = r.readLine()) != null) {
            // Skipping through comments
            if (!line.startsWith("--")) {
                qsb.append(line + "\n");
            }
        }
        deal(qsb.toString());
    }

    public void deal(String line) {

        String command = "";
        for (String oneCmd : line.split(";")) {
            if (org.apache.commons.lang.StringUtils.endsWith(oneCmd, "\\")) {
                command += org.apache.commons.lang.StringUtils.chop(oneCmd) + ";";
                continue;
            } else {
                command += oneCmd;
            }
            if (org.apache.commons.lang.StringUtils.isBlank(command)) {
                continue;
            }
            System.out.println(command);
            System.out.println("=======");
            command = "";
        }
    }


    @Test
    public void split() {
        String sql = "abc; abcd;\n absc;\tabcde ';';   abcde\';\";";

        String[] split = sql.split("[\\s]*(?!'|\");(?!'|\")[\\s]*");
        for (String s : split) {
            System.out.println(s);
        }
        Assert.assertEquals(5, split.length);

        sql = "--我是注释  \n     -------注释来了  \n    --        我也是注释\n select * from table 1 \n 哈哈--我的注释在后面";


        split = sql.split("\n");

        Arrays.stream(split).forEach(s -> {

            // System.out.println(s);
            s = s.replaceAll("\\s*-+.*", "");
            System.out.println(s);


        });


    }

    @Test
    public void lineSplit() {

        String regex = "\\s*-+.*\n$";
        String blank = "";
        Assert.assertEquals("--\n".replaceAll(regex, blank), blank);
        Assert.assertEquals("abc--\n".replaceAll(regex, blank), "abc");
        Assert.assertEquals("---注释\n".replaceAll(regex, blank), blank);
        Assert.assertEquals("   \t -----注释\n".replaceAll(regex, blank), blank);


        Assert.assertEquals("SELECT /*+ REPARTITION(1) */ md5".replaceAll("/\\*.*\\*/", blank), "SELECT  md5");
        Assert.assertEquals("SELECT /****/ md5".replaceAll("/\\*.*\\*/", blank), "SELECT  md5");
        Assert.assertEquals("SELECT /**/ md5".replaceAll("/\\*.*\\*/", blank), "SELECT  md5");
        System.out.println(";    \n".matches(";[ ]*\n+"));

    }


    @Test
    public void prestoTest() {


    }

}
