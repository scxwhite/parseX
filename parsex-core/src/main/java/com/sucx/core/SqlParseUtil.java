package com.sucx.core;

import com.sucx.common.enums.SqlEnum;
import com.sucx.common.exceptions.SqlParseException;
import com.sucx.common.model.Result;
import com.sucx.common.model.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.Tuple3;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/26
 */
public class SqlParseUtil {

    private static final Log LOG = LogFactory.getLog(SqlParseUtil.class);


    private static Map<SqlEnum, SqlParse> sqlParseMap = new ConcurrentHashMap<>(3);


    public static Result parsePrestoSql(String sqlText) throws SqlParseException {
        return parse(SqlEnum.PRESTO, sqlText);
    }

    public static Result parseHiveSql(String sqlText) throws SqlParseException {
        return parse(SqlEnum.HIVE, sqlText);
    }

    public static Result parseSparkSql(String sqlText) throws SqlParseException {
        return parse(SqlEnum.SPARK, sqlText);
    }

    /**
     * 解析sql入口
     *
     * @param sqlEnum sql类型
     * @param sqlText sql内容
     * @return Result
     * @throws SqlParseException 解析异常
     */
    private static Result parse(SqlEnum sqlEnum, String sqlText) throws SqlParseException {
        Result result;
        switch (sqlEnum) {
            case SPARK:
                try {
                    result = getSqlParse(sqlEnum).parse(sqlText);
                } catch (Exception e) {
                    LOG.error("spark引擎解析异常,准备使用hive引擎解析:" + sqlText);
                    try {
                        result = getSqlParse(SqlEnum.HIVE).parse(sqlText);
                    } catch (Exception e1) {
                        throw new SqlParseException(e);
                    }
                }
                return result;
            case HIVE:
                try {
                    result = getSqlParse(sqlEnum).parse(sqlText);
                } catch (Exception e) {
                    LOG.error("hive引擎解析异常,准备使用spark引擎解析:" + sqlText);
                    try {
                        result = getSqlParse(SqlEnum.SPARK).parse(sqlText);
                    } catch (Exception e1) {
                        throw new SqlParseException(e);
                    }
                }
                return result;

            case PRESTO:
                result = getSqlParse(sqlEnum).parse(sqlText);
                return result;
            default:
                throw new IllegalArgumentException("not support sqlEnum type :" + sqlEnum.name());

        }
    }


    private static SqlParse getSqlParse(SqlEnum sqlEnum) {
        SqlParse sqlParse = sqlParseMap.get(sqlEnum);
        if (sqlParse == null) {
            synchronized (SqlParseUtil.class) {
                sqlParse = sqlParseMap.get(sqlEnum);
                if (sqlParse == null) {
                    switch (sqlEnum) {
                        case PRESTO:
                            sqlParse = new PrestoSqlParse();
                            break;
                        case SPARK:
                            sqlParse = new SparkSQLParse();
                            break;
                        case HIVE:
                            sqlParse = new HiveSQLParse();
                            break;
                        default:
                            throw new IllegalArgumentException("not support sqlEnum type :" + sqlEnum.name());

                    }
                    sqlParseMap.put(sqlEnum, sqlParse);
                }
            }
        }
        return sqlParse;
    }

    public static void print(Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> tuple3) {
        if (tuple3 == null) {
            return;
        }
        print(tuple3._2(), tuple3._2(), tuple3._3(), false);
    }

    public static void print(Result result) {
        if (result == null) {
            return;
        }
        print(result.getInputSets(), result.getOutputSets(), result.getTempSets(), result.isJoin());

    }

    private static void print(Set<TableInfo> inputTable, Set<TableInfo> outputTable, Set<TableInfo> tempTable, boolean join) {

        LOG.info("是否包含join:" + join);
        LOG.info("输入表有:");
        for (TableInfo table : inputTable) {
            LOG.info(table);
        }

        LOG.info("输出表有:");

        for (TableInfo table : outputTable) {
            LOG.info(table);
        }

        LOG.info("临时表:");

        for (TableInfo table : tempTable) {
            LOG.info(table);
        }

    }

}
