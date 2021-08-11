package com.sucx.core;

import com.sucx.common.exceptions.SqlParseException;
import com.sucx.common.model.Result;

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
     * @return Result 结果
     */
    Result parse(String sqlText) throws SqlParseException;

}
