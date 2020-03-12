package com.tuya.core;

import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.TableInfo;
import scala.Tuple3;

import java.util.HashSet;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/29
 */
public interface SqlParse {


    /**
     * 血缘解析入口
     *
     * @param sqlText sql
     * @return Tuple3<set1, set2, set3> set1:输入表，set2:输出表，set3:临时表
     */
    Tuple3<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>> parse(String sqlText) throws SqlParseException;

}
