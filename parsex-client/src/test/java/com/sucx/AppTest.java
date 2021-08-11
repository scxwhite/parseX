package com.sucx;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageLogger;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.joor.Reflect;
import org.junit.Test;

import java.util.HashMap;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() throws Exception {

        LineageLogger logger = new LineageLogger();

        HiveConf hiveConf = new HiveConf();


        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost/metastore", "hive-conf.xml");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver", "hive-conf.xml");
        hiveConf.set("javax.jdo.option.ConnectionUserName", "root", "hive-conf.xml");
        hiveConf.set("javax.jdo.option.ConnectionPassword", "moye", "hive-conf.xml");
        hiveConf.set("fs.defaultFS", "hdfs://127.0.0.1:8020", "hdfs-site.xml");
        hiveConf.set("_hive.hdfs.session.path", "hdfs://127.0.0.1:8020/tmp", "hive-conf.xml");
        hiveConf.set("_hive.local.session.path", "hdfs://127.0.0.1:8020/tmp", "hive-conf.xml");
        hiveConf.set("hive.in.test", "true", "hive-conf.xml");


        String sql = "insert overwrite table sucx.test  select * from sucx.test2";
        QueryState queryState = new QueryState(hiveConf);

        Context context = new Context(hiveConf);

        SessionState sessionState = new SessionState(hiveConf);

        SessionState.setCurrentSessionState(sessionState);

        ASTNode astNode = ParseUtils.parse(sql, context);

        BaseSemanticAnalyzer analyzer = SemanticAnalyzerFactory.get(queryState, astNode);

        analyzer.analyze(astNode, context);

        Schema schema = Reflect.onClass(Driver.class).call("getSchema", analyzer, hiveConf).get();

        QueryPlan queryPlan = new QueryPlan(sql, analyzer, 0L, null, queryState.getHiveOperation(), schema);



        HookContext hookContext = new HookContext(queryPlan, queryState,
                new HashMap<>(), "sucx", "",
                "", "", "", "",
                true, null);

        hookContext.setUgi(UserGroupInformation.getCurrentUser());
        logger.run(hookContext);


    }

}
