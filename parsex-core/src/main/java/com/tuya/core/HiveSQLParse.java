package com.tuya.core;

import com.tuya.core.enums.OperatorType;
import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.TableInfo;
import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;
import scala.Tuple4;

import java.util.*;

/**
 * desc:
 *
 * @author scx
 * @create 2020/02/29
 */
public class HiveSQLParse extends AbstractSqlParse implements NodeProcessor {

    /**
     * 临时输入表
     */
    private HashSet<TableInfo> inputTableList;

    /**
     * 临时输出表
     */
    private HashSet<TableInfo> outputTableList;

    /**
     * 临时表
     */
    private HashSet<TableInfo> withTableList;

    /**
     * 临时Db
     */
    private String currentDb;

    @Override
    public Object process(Node nd, Stack stack, NodeProcessorCtx procCtx, Object... nodeOutputs) {
        ASTNode ast = (ASTNode) nd;
        switch (ast.getToken().getType()) {
            //create语句
            case HiveParser.TOK_CREATETABLE: {
                String tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                outputTableList.add(new TableInfo(tableName, OperatorType.CREATE, currentDb, new HashSet<>()));
                break;
            }
            //insert语句
            case HiveParser.TOK_TAB: {
                String tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                outputTableList.add(new TableInfo(tableName, OperatorType.WRITE, currentDb, new HashSet<>()));
                break;
            }
            //from语句
            case HiveParser.TOK_TABREF: {
                ASTNode tabTree = (ASTNode) ast.getChild(0);
                String tableName = (tabTree.getChildCount() == 1) ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
                inputTableList.add(new TableInfo(tableName, OperatorType.READ, currentDb, new HashSet<>()));
                break;
            }
            // with.....语句
            case HiveParser.TOK_CTE: {
                for (int i = 0; i < ast.getChildCount(); i++) {
                    ASTNode temp = (ASTNode) ast.getChild(i);
                    String tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
                    withTableList.add(new TableInfo(tableName, OperatorType.READ, "temp", new HashSet<>()));
                }
                break;
            }
            //ALTER 语句
            case HiveParser.TOK_ALTERTABLE: {
                String tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                inputTableList.add(new TableInfo(tableName, OperatorType.ALTER, currentDb, new HashSet<>()));
                break;
            }
            case HiveParser.TOK_SWITCHDATABASE: {
                this.currentDb = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
                break;
            }
            default: {
                return null;
            }
        }
        return null;
    }

    @Override
    protected String replaceNotes(String sqlText) {
        StringBuilder builder = new StringBuilder();
        String lineBreak = "\n";
        for (String line : sqlText.split(lineBreak)) {
            //udf 添加的去掉，目前无法解析，会抛异常
            if (line.toLowerCase().startsWith("add jar") || line.toLowerCase().startsWith("set")) {
                int splitIndex = line.indexOf(Constants.SEMICOLON);
                if (splitIndex != -1 && splitIndex + 1 != line.length()) {
                    builder.append(line.substring(splitIndex + 1)).append(lineBreak);
                }
            } else {
                builder.append(line).append(lineBreak);
            }
        }
        return super.replaceNotes(builder.toString());
    }

    @Override
    protected Tuple4<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>, String> parseInternal(String sqlText, String currentDb) throws SqlParseException {
        ParseDriver pd = new ParseDriver();
        ASTNode tree;
        try {
            tree = pd.parse(sqlText);
        } catch (ParseException e) {
            throw new SqlParseException(e);
        }
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        inputTableList = new HashSet<>();
        outputTableList = new HashSet<>();
        withTableList = new HashSet<>();
        this.currentDb = currentDb;
        Map<Rule, NodeProcessor> rules = new LinkedHashMap<>();

        GraphWalker ogw = new DefaultGraphWalker(new DefaultRuleDispatcher(this, rules, null));

        ArrayList<Node> topNodes = new ArrayList<>();
        topNodes.add(tree);
        try {
            ogw.startWalking(topNodes, null);
        } catch (SemanticException e) {
            throw new RuntimeException(e);
        }
        return new Tuple4<>(inputTableList, outputTableList, withTableList, this.currentDb);
    }
}
