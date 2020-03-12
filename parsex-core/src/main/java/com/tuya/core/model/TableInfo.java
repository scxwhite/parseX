package com.tuya.core.model;

import com.tuya.core.enums.OperatorType;

import java.util.HashSet;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/26
 */
public class TableInfo {

    /**
     * 表名
     */
    private String name;

    /**
     * 库名
     */
    private String dbName;

    private OperatorType type;

    private HashSet<String> columns;

    private String limit;

    public TableInfo(String name, String dbName, OperatorType type, HashSet<String> columns) {
        this.name = name;
        this.dbName = dbName;
        this.type = type;
        this.columns = new HashSet<>(columns);
        columns.clear();
    }

    public TableInfo(String dbAndTableName, OperatorType type, String defaultDb, HashSet<String> columns) {
        if (dbAndTableName.contains(".")) {
            int index = dbAndTableName.indexOf(".");
            this.dbName = dbAndTableName.substring(0, index);
            this.name = dbAndTableName.substring(index + 1);
        } else {
            this.name = dbAndTableName;
            this.dbName = defaultDb;
        }
        this.columns = new HashSet<>(columns);
        this.type = type;
        columns.clear();
    }

    public String getName() {
        return name;
    }


    public String getDbName() {
        return dbName;
    }

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }

    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();


        this.columns.forEach(columns -> builder.append(columns).append(" "));

        return dbName + "." + name + "[" + type.name() + "]\ncolumn=[ " + builder.toString() + " ]\nlimit=" + limit + "\n";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TableInfo)) {
            return false;
        }

        TableInfo info = (TableInfo) obj;
        return this.dbName.equals(info.dbName) && this.name.equals(info.name) && this.type == info.type;
    }

    @Override
    public int hashCode() {
        return this.dbName.hashCode() + this.name.hashCode() + this.type.hashCode();
    }
}
