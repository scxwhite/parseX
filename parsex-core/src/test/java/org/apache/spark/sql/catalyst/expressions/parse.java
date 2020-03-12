package org.apache.spark.sql.catalyst.expressions;

import com.tuya.core.SparkSQLParse;
import com.tuya.core.SqlParse;
import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.TableInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/24
 */
public class parse {

    String sql = "CREATE EXTERNAL TABLE IF NOT EXISTS `bi_dw`.`dws_tools_jira_bug_1d2`(`date_code` int, `user` string, `name` string, `bug` bigint, `major_depid` string, `major_depname` string, `today_new_add` bigint, `today_new_add2` bigint) COMMENT '程龙jira统计' partitioned by(dt string) STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
            "LOCATION 'cosn://tuya-big-data-1254153901/bi/bi_dw/dws_tools_jira_bug_1d2' ;INSERT overwrite TABLE bi_dw.dws_tools_jira_bug_1d2 partition(dt=20200308) SELECT 20200308 date_code,\n" +
            "       *\n" +
            "  FROM (\n" +
            "        SELECT assignee AS `user` ,\n" +
            "               max(name) name ,\n" +
            "               count(id) AS bug ,\n" +
            "               max(major_depid) major_depid ,\n" +
            "               max(major_depname) major_depname,\n" +
            "               sum(CASE WHEN created >=concat('2020-03-07',' 08:00:00') AND created <= concat('2020-03-08',' 08:00:00') THEN 1 ELSE 0 END) AS today_new_add ,\n" +
            "               sum(CASE WHEN created >=concat('2020-03-07',' 08:00:00') AND created <= concat('2020-03-08',' 08:00:00') AND yujitime IS NULL THEN 1 ELSE 0 END) AS today_new_add2\n" +
            "          FROM (\n" +
            "                SELECT ss.*,\n" +
            "                       ee.datevalue AS yujitime\n" +
            "                  FROM (\n" +
            "                        SELECT bb.id id,\n" +
            "                               bb.creator creator,\n" +
            "                               bb.assignee assignee,\n" +
            "                               bb.reporter reporter,\n" +
            "                               bb.updated updated,\n" +
            "                               bb.created created,\n" +
            "                               bb.resolution resolution,\n" +
            "                               bb.resolutiondate resolutiondate,\n" +
            "                               bb.priority priority,\n" +
            "                               bb.project project,\n" +
            "                               bb.issuetype issuetype,\n" +
            "                               bb.issuestatus issuestatus,\n" +
            "                               aa.pname status,\n" +
            "                               yy.major_depid,\n" +
            "                               yy.major_depname,\n" +
            "                               yy.name\n" +
            "                          FROM bi_ods.ods_jira_issuestatus aa,\n" +
            "                               bi_ods.ods_jira_jiraissue bb,\n" +
            "                               (\n" +
            "                                SELECT *\n" +
            "                                  FROM (\n" +
            "                                        SELECT name,\n" +
            "                                               email,\n" +
            "                                               major_depid,\n" +
            "                                               major_depname,\n" +
            "                                               row_number() over(PARTITION BY email ORDER BY cast(major_depid AS int) ASC) rk\n" +
            "                                          FROM bi_dw_temp.tuya_employee\n" +
            "                                       ) mm\n" +
            "                                 WHERE rk =1\n" +
            "                               ) yy\n" +
            "                         WHERE aa.dt=20200308\n" +
            "                           AND bb.dt=20200308\n" +
            "                           AND aa.id = bb.issuestatus\n" +
            "                           AND bb.assignee=yy.email\n" +
            "                           AND bb.issuetype = '10004'\n" +
            "                           AND bb.issuestatus IN ('1', '4')\n" +
            "                           AND bb.assignee LIKE '%@tuya.com'\n" +
            "                       ) ss\n" +
            "                  LEFT JOIN (\n" +
            "                        SELECT eee.id,\n" +
            "                               eee.ISSUE,\n" +
            "                               eee.DATEVALUE\n" +
            "                          FROM bi_ods.ods_jira_customfieldvalue eee\n" +
            "                         WHERE CUSTOMFIELD = '10800'\n" +
            "                           AND eee.dt=20200308\n" +
            "                       ) AS ee\n" +
            "                    ON cast(ss.id AS string) = ee.ISSUE\n" +
            "               ) cc\n" +
            "         GROUP BY assignee\n" +
            "       ) dd\n" +
            " ORDER BY dd.bug DESC,\n" +
            "          dd.today_new_add DESC;\n";

    @Test
    public void parse() throws SqlParseException {

        SqlParse sqlParse = new SparkSQLParse();

        Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> tuple3 = sqlParse.parse(sql);

        print(tuple3);

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


    private void print(Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> tuple3) {

        System.out.println("输入表有:");
        for (TableInfo table : tuple3._1()) {
            System.out.print(table);
        }

        System.out.println("输出表有:");

        for (TableInfo table : tuple3._2()) {
            System.out.print(table);
        }

        System.out.println("临时表:");

        for (TableInfo table : tuple3._3()) {
            System.out.print(table);
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
