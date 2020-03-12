package com.tuya.core;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.tuya.core.enums.OperatorType;
import com.tuya.core.exceptions.SqlParseException;
import com.tuya.core.model.TableInfo;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * desc:
 *
 * @author scx
 * @create 2020/03/09
 */
public class PrestoSqlParse extends AbstractSqlParse {


    private final String columnSplit = ",";
    private HashSet<TableInfo> inputTables;
    private HashSet<TableInfo> outputTables;
    private HashSet<TableInfo> tempTables;
    private HashSet<String> columns;
    private String currentDb;

    /**
     * select 字段表达式中获取字段
     *
     * @param expression
     * @return
     */
    private String getColumn(Expression expression) throws SqlParseException {
        if (expression instanceof IfExpression) {
            IfExpression ifExpression = (IfExpression) expression;
            List<Expression> list = new ArrayList<>();
            list.add(ifExpression.getCondition());
            list.add(ifExpression.getTrueValue());
            ifExpression.getFalseValue().ifPresent(list::add);
            return getString(list);
        } else if (expression instanceof Identifier) {
            Identifier identifier = (Identifier) expression;
            return identifier.getValue();
        } else if (expression instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) expression;
            StringBuilder columns = new StringBuilder();
            List<Expression> arguments = call.getArguments();
            int size = arguments.size();
            for (int i = 0; i < size; i++) {
                Expression exp = arguments.get(i);
                if (i == 0) {
                    columns.append(getColumn(exp));
                } else {
                    columns.append(getColumn(exp)).append(columnSplit);
                }
            }
            return columns.toString();
        } else if (expression instanceof ComparisonExpression) {
            ComparisonExpression compare = (ComparisonExpression) expression;
            return getString(compare.getLeft(), compare.getRight());
        } else if (expression instanceof Literal) {
            return "";
        } else if (expression instanceof Cast) {
            Cast cast = (Cast) expression;
            return getColumn(cast.getExpression());
        } else if (expression instanceof DereferenceExpression) {
            DereferenceExpression reference = (DereferenceExpression) expression;
            return reference.toString();
        } else if (expression instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression binaryExpression = (ArithmeticBinaryExpression) expression;
            return getString(binaryExpression.getLeft(), binaryExpression.getRight());
        } else if (expression instanceof SearchedCaseExpression) {
            SearchedCaseExpression caseExpression = (SearchedCaseExpression) expression;
            List<Expression> exps = caseExpression.getWhenClauses().stream().map(whenClause -> (Expression) whenClause).collect(Collectors.toList());
            caseExpression.getDefaultValue().ifPresent(exps::add);
            return getString(exps);
        } else if (expression instanceof WhenClause) {
            WhenClause whenClause = (WhenClause) expression;
            return getString(whenClause.getOperand(), whenClause.getResult());
        } else if (expression instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) expression;
            return likePredicate.getValue().toString();
        } else if (expression instanceof InPredicate) {
            InPredicate predicate = (InPredicate) expression;
            return predicate.getValue().toString();
        } else if (expression instanceof SubscriptExpression) {
            SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
            return getColumn(subscriptExpression.getBase());
        } else if (expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression logicExp = (LogicalBinaryExpression) expression;
            return getString(logicExp.getLeft(), logicExp.getRight());
        } else if (expression instanceof IsNullPredicate) {
            IsNullPredicate isNullExp = (IsNullPredicate) expression;
            return isNullExp.getValue().toString();
        }

        throw new SqlParseException("无法识别的表达式:" + expression.getClass().getName());
        //   return expression.toString();
    }


    private String getString(Expression... exps) throws SqlParseException {
        return getString(Arrays.stream(exps).collect(Collectors.toList()));
    }

    private String getString(List<Expression> exps) throws SqlParseException {
        StringBuilder builder = new StringBuilder();
        for (Expression exp : exps) {
            builder.append(getColumn(exp)).append(columnSplit);
        }
        return builder.toString();
    }

    /**
     * node 节点的遍历
     *
     * @param node
     */
    private void checkNode(Node node) throws SqlParseException {
        if (node instanceof QuerySpecification) {
            QuerySpecification query = (QuerySpecification) node;
            Relation from = query.getFrom().orElse(null);
            if (from != null) {
                if (from instanceof Table) {
                    List<SelectItem> selectItems = query.getSelect().getSelectItems();
                    for (SelectItem item : selectItems) {
                        if (item instanceof SingleColumn) {
                            columns.add(getColumn(((SingleColumn) item).getExpression()));
                        } else if (item instanceof AllColumns) {
                            columns.add(item.toString());
                        } else {
                            throw new SqlParseException("unknow column type:" + item.getClass().getName());
                        }
                    }
                    columns = (HashSet<String>) columns.stream().flatMap(column -> Arrays.stream(column.split(columnSplit))).collect(Collectors.toSet());
                    Table table = (Table) from;
                    TableInfo info = new TableInfo(table.getName().toString(), OperatorType.READ, currentDb, columns);
                    query.getLimit().ifPresent(info::setLimit);
                    inputTables.add(info);
                }
            }
            loopNode(node.getChildren().stream().filter(child -> !(child instanceof Table)).collect(Collectors.toList()));

        } else if (node instanceof TableSubquery) {
            loopNode(node.getChildren());
        } else if (node instanceof AliasedRelation) {
            AliasedRelation alias = (AliasedRelation) node;
            String value = alias.getAlias().getValue();
            tempTables.add(new TableInfo(value, OperatorType.READ, currentDb, columns));
            loopNode(node.getChildren());
        } else if (node instanceof Query) {
            loopNode(node.getChildren());
        } else if (node instanceof Join) {
            loopNode(node.getChildren());
        } else if (node instanceof Union) {
            loopNode(node.getChildren());
        } else if (node instanceof LikePredicate || node instanceof NotExpression
                || node instanceof IfExpression || node instanceof LogicalBinaryExpression
                || node instanceof ComparisonExpression || node instanceof GroupBy
                || node instanceof OrderBy || node instanceof Select) {
            print(node.getClass().getName());
        } else if (node instanceof With) {
            With withNode = (With) node;
            loopNode(withNode.getChildren());
        } else if (node instanceof WithQuery) {
            WithQuery withQuery = (WithQuery) node;
            tempTables.add(new TableInfo(withQuery.getName().getValue(), OperatorType.READ, currentDb, columns));
            loopNode(withQuery.getChildren());
        } else {
            throw new SqlParseException("unknow node type:" + node.getClass().getName());
        }
    }

    private void loopNode(List<? extends Node> children) throws SqlParseException {
        for (Node node : children) {
            this.checkNode(node);
        }
    }

    /**
     * statement 过滤 只识别select 语句
     *
     * @param statement
     * @throws SqlParseException
     */
    private void check(Statement statement) throws SqlParseException {
        if (statement instanceof Query) {
            Query query = (Query) statement;
            List<Node> children = query.getChildren();
            for (Node child : children) {
                checkNode(child);
            }
        } else if (statement instanceof Use) {
            Use use = (Use) statement;
            this.currentDb = use.getSchema().getValue();
        } else if (statement instanceof ShowColumns) {
            ShowColumns show = (ShowColumns) statement;
            inputTables.add(new TableInfo(show.getTable().toString(), OperatorType.READ, currentDb, columns));
        } else {
            throw new SqlParseException("unSupport statement:" + statement.getClass().getName());
        }
    }


    @Override
    protected Tuple4<HashSet<TableInfo>, HashSet<TableInfo>, HashSet<TableInfo>, String> parseInternal(String sqlText, String currentDb) throws SqlParseException {
        this.currentDb = currentDb;
        this.inputTables = new HashSet<>();
        this.outputTables = new HashSet<>();
        this.tempTables = new HashSet<>();
        this.columns = new HashSet<>();
        try {
            check(new SqlParser().createStatement(sqlText, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL)));
        } catch (ParsingException e) {
            throw new SqlParseException("parse sql exception:" + e.getMessage());
        }
        return new Tuple4<>(inputTables, outputTables, tempTables, this.currentDb);
    }
}
