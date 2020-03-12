package com.tuya.core;

import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.TableInfo;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple3;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/29
 */
public abstract class AbstractSqlParse implements SqlParse {


    /**
     * 替换sql注释
     *
     * @param sqlText sql
     * @return 替换后的sl
     */
    protected String replaceNotes(String sqlText) {
        StringBuilder newSql = new StringBuilder();
        String lineBreak = "\n";
        String empty = "";
        String trimLine;
        for (String line : sqlText.split(lineBreak)) {
            trimLine = line.trim();
            if (!trimLine.startsWith("--") && !trimLine.startsWith("download")) {
                //过滤掉行内注释
                line = line.replaceAll("/\\*.*\\*/", empty);
                if (StringUtils.isNotBlank(line)) {
                    newSql.append(line).append(lineBreak);
                }
            }
        }
        return newSql.toString();
    }


    /**
     * ;分割多段sql
     *
     * @param sqlText sql
     * @return
     */
    protected ArrayList<String> splitSql(String sqlText) {
        String[] sqlArray = sqlText.split(Constants.SEMICOLON);
        ArrayList<String> newSqlArray = new ArrayList<>(sqlArray.length);
        String command = "";
        int arrayLen = sqlArray.length;
        String oneCmd;
        for (int i = 0; i < arrayLen; i++) {
            oneCmd = sqlArray[i];
            boolean keepSemicolon = (oneCmd.endsWith("'") && i + 1 < arrayLen && sqlArray[i + 1].startsWith("'"))
                    || (oneCmd.endsWith("\"") && i + 1 < arrayLen && sqlArray[i + 1].startsWith("\""));
            if (oneCmd.endsWith("\\")) {
                command += org.apache.commons.lang.StringUtils.chop(oneCmd) + Constants.SEMICOLON;
                continue;
            } else if (keepSemicolon) {
                command += oneCmd + Constants.SEMICOLON;
                continue;
            } else {
                command += oneCmd;
            }
            if (StringUtils.isBlank(command)) {
                continue;
            }
            newSqlArray.add(command);
            command = "";
        }
        return newSqlArray;
    }


    @Override
    public Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse(String sqlText) throws SqlParseException {

        ArrayList<String> sqlArray = this.splitSql(this.replaceNotes(sqlText));
        HashSet<TableInfo> inputTables = new HashSet<>();
        HashSet<TableInfo> outputTables = new HashSet<>();
        HashSet<TableInfo> tempTables = new HashSet<>();

        String currentDb = "default";
        for (String sql : sqlArray) {
            if (sql.charAt(sql.length() - 1) == ';') {
                sql = sql.substring(0, sql.length() - 1);
            }
            if (StringUtils.isBlank(sql)) {
                continue;
            }
            Tuple4<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>, String> subTuple = this.parseInternal(sql, currentDb);
            inputTables.addAll(subTuple._1());
            outputTables.addAll(subTuple._2());
            tempTables.addAll(subTuple._3());
            currentDb = subTuple._4();
        }

        tempTables.forEach(table -> {
            Iterator<TableInfo> iterator = inputTables.iterator();
            while (iterator.hasNext()) {
                TableInfo checkTable = iterator.next();
                if (checkTable.getName().equals(table.getName())) {
                    iterator.remove();
                    break;
                }
            }
        });

        return new Tuple3<>(inputTables, outputTables, tempTables);
    }

    /**
     * 抽象解析
     *
     * @param sqlText   sql
     * @param currentDb 当前db
     * @return tuple4
     * @throws SqlParseException
     */
    protected abstract Tuple4<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>, String> parseInternal(String sqlText, String currentDb) throws SqlParseException;

    protected void print(String plan) {
        System.out.println(("************ignore plan******\n" + plan));
    }
}
