package com.sucx.common.exceptions;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/29
 */
public class SqlParseException extends Exception {

    public SqlParseException(Exception e) {
        super(e);
    }

    public SqlParseException(String e) {
        super(e);
    }
    public SqlParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
