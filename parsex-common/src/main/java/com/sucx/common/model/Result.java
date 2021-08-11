package com.sucx.common.model;

import java.util.Set;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/26
 */
public class Result {

    /**
     * 输入表
     */
    private Set<TableInfo> inputSets;
    /**
     * 输出表
     */
    private Set<TableInfo> outputSets;

    /**
     * 临时表
     */
    private Set<TableInfo> tempSets;

    /**
     * 是否包含join操作
     */
    private boolean join;

    public Result(){}

    public Result(Set<TableInfo> inputSets, Set<TableInfo> outputSets, Set<TableInfo> tempSets) {
        this(inputSets, outputSets, tempSets, false);
    }


    public Result(Set<TableInfo> inputSets, Set<TableInfo> outputSets, Set<TableInfo> tempSets, boolean join) {
        this.inputSets = inputSets;
        this.outputSets = outputSets;
        this.tempSets = tempSets;
        this.join = join;
    }


    @Override
    public String toString() {
        StringBuilder inputStr = new StringBuilder("*************************\n输入表为:\n");
        StringBuilder outputStr = new StringBuilder("输出表为:\n");
        StringBuilder tempStr = new StringBuilder("临时表为:\n");

        inputSets.forEach(input -> inputStr.append(input.toString()).append(" ").append("\n"));
        outputSets.forEach(input -> outputStr.append(input.toString()).append(" "));
        tempSets.forEach(input -> tempStr.append(input.toString()).append(" "));

        return inputStr.append(outputStr).append(tempStr).toString();
    }


    public boolean isJoin() {
        return join;
    }

    public Set<TableInfo> getTempSets() {
        return tempSets;
    }

    public Set<TableInfo> getInputSets() {
        return inputSets;
    }

    public void setInputSets(Set<TableInfo> inputSets) {
        this.inputSets = inputSets;
    }

    public Set<TableInfo> getOutputSets() {
        return outputSets;
    }

    public void setOutputSets(Set<TableInfo> outputSets) {
        this.outputSets = outputSets;
    }
}
