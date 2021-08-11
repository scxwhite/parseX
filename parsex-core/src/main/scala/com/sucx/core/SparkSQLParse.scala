package com.sucx.core

import java.util.{HashSet => JSet}

import com.sucx.common.enums.OperatorType
import com.sucx.common.model.TableInfo
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, RefreshTable}
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConversions._

class SparkSQLParse extends AbstractSqlParse {


  private[this] def resolveLogicPlan(plan: LogicalPlan) = {
    val inputTables = new JSet[TableInfo]()
    val outputTables = new JSet[TableInfo]()
    val tmpTables = new JSet[TableInfo]()
    resolveLogic(plan, inputTables, outputTables, tmpTables)
    Tuple3(inputTables, outputTables, tmpTables)
  }


  /* def getColumnAuto(exps: Expression*): String = {
     getColumn(exps)
   }

   def getColumn(expSeq: Seq[Expression]): String = {
     val columns = new StringBuilder
     expSeq.foreach(exp => {
       columns.append(resolveExp(exp)).append(",")
     })
     columns.toString()
   }

   private[this] def resolveExp(exp: Expression): String = {
     val column = ""
     exp match {
       case alias: Alias =>
         return resolveExp(alias.child)
       case divide: Divide =>
         return getColumnAuto(divide.left, divide.right)
       case cast: Cast =>
         return resolveExp(cast.child)
       case unresolvedFun: UnresolvedFunction =>
         return getColumn(unresolvedFun.children)
       case unresolvedAttribute: UnresolvedAttribute =>
         return unresolvedAttribute.name
       case literal: Literal =>
         print(literal.sql)
       case caseWhen: CaseWhen =>
         return getColumn(caseWhen.children)
       case in: In =>
         return getColumn(in.children)
       case equalTo: EqualTo =>
         return getColumnAuto(equalTo.left, equalTo.right)
       case unresolvedAlias: UnresolvedAlias =>
         return getColumnAuto(unresolvedAlias.child)
       case unresolvedStar: UnresolvedStar =>
         return unresolvedStar.toString()
       case multiAlias: MultiAlias =>
         return resolveExp(multiAlias.child)

       case _ =>
         throw new SqlParseException("无法识别的exp:" + exp.getClass.getName)
     }
     column
   }*/


  private[this] def resolveLogic(plan: LogicalPlan, inputTables: JSet[TableInfo], outputTables: JSet[TableInfo], tmpTables: JSet[TableInfo]): Unit = {
    plan match {

      case plan: Project =>
        val project: Project = plan.asInstanceOf[Project]
        /*        val columnsSet = new JSet[String]()
                project.projectList.foreach(exp => {
                  columnsSet.add(resolveExp(exp))
                })

                columnsStack.push(columnsSet)*/
        resolveLogic(project.child, inputTables, outputTables, tmpTables)
      case plan: Union =>
        val project: Union = plan.asInstanceOf[Union]
        for (child <- project.children) {
          resolveLogic(child, inputTables, outputTables, tmpTables)
        }
      case plan: Join =>
        val project: Join = plan.asInstanceOf[Join]
        resolveLogic(project.left, inputTables, outputTables, tmpTables)
        resolveLogic(project.right, inputTables, outputTables, tmpTables)

      case plan: Aggregate =>
        val project: Aggregate = plan.asInstanceOf[Aggregate]
        /*        val columnsSet = new JSet[String]()
                project.aggregateExpressions.foreach(exp => {
                  columnsSet.add(resolveExp(exp))
                })
                columnsStack.push(columnsSet)*/
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: Filter =>
        val project: Filter = plan.asInstanceOf[Filter]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: Generate =>
        val project: Generate = plan.asInstanceOf[Generate]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: RepartitionByExpression =>
        val project: RepartitionByExpression = plan.asInstanceOf[RepartitionByExpression]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: SerializeFromObject =>
        val project: SerializeFromObject = plan.asInstanceOf[SerializeFromObject]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: MapPartitions =>
        val project: MapPartitions = plan.asInstanceOf[MapPartitions]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: DeserializeToObject =>
        val project: DeserializeToObject = plan.asInstanceOf[DeserializeToObject]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: Repartition =>
        val project: Repartition = plan.asInstanceOf[Repartition]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: Deduplicate =>
        val project: Deduplicate = plan.asInstanceOf[Deduplicate]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: Window =>
        val project: Window = plan.asInstanceOf[Window]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: MapElements =>
        val project: MapElements = plan.asInstanceOf[MapElements]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: TypedFilter =>
        val project: TypedFilter = plan.asInstanceOf[TypedFilter]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: Distinct =>
        val project: Distinct = plan.asInstanceOf[Distinct]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: SubqueryAlias =>
        val project: SubqueryAlias = plan.asInstanceOf[SubqueryAlias]
        val childInputTables = new JSet[TableInfo]()
        val childOutputTables = new JSet[TableInfo]()

        resolveLogic(project.child, childInputTables, childOutputTables, tmpTables)
        if (childInputTables.size() > 1) {
          for (table <- childInputTables) inputTables.add(table)
        } else if (childInputTables.size() == 1) {
          val tableInfo: TableInfo = childInputTables.iterator().next()
          tableAliaMap.put(project.alias, tableInfo.getDbName + "." + tableInfo.getName)
          inputTables.add(new TableInfo(tableInfo.getName, tableInfo.getDbName, tableInfo.getType, splitColumn(tableInfo.getColumns, tableAliaMap)))
        }
        tmpTables.add(buildTableInfo(project.alias, this.currentDb, OperatorType.READ))

      case plan: UnresolvedCatalogRelation =>
        val project: UnresolvedCatalogRelation = plan.asInstanceOf[UnresolvedCatalogRelation]
        val identifier: TableIdentifier = project.tableMeta.identifier
        inputTables.add(buildTableInfo(identifier.table, identifier.database.getOrElse(this.currentDb), OperatorType.READ))

      case plan: UnresolvedRelation =>
        val project: UnresolvedRelation = plan.asInstanceOf[UnresolvedRelation]
        inputTables.add(buildTableInfo(project.tableIdentifier.table, project.tableIdentifier.database.getOrElse(this.currentDb), OperatorType.READ))

      case plan: InsertIntoTable =>
        val project: InsertIntoTable = plan.asInstanceOf[InsertIntoTable]
        plan.table match {
          case relation: UnresolvedRelation =>
            val table: TableIdentifier = relation.tableIdentifier
            outputTables.add(buildTableInfo(table.table, table.database.getOrElse(this.currentDb), OperatorType.WRITE))
          case _ =>
            throw new RuntimeException("无法解析的插入逻辑语法树:" + plan.table)
        }

        resolveLogic(project.query, inputTables, outputTables, tmpTables)

      case plan: CreateTable =>
        val project: CreateTable = plan.asInstanceOf[CreateTable]
        if (project.query.isDefined) {
          resolveLogic(project.query.get, inputTables, outputTables, tmpTables)
        }
        val columnsSet = new JSet[String]()
        project.tableDesc.schema.fields.foreach(field => {
          columnsSet.add(field.name)
        })
        columnsStack.push(columnsSet)
        val tableIdentifier: TableIdentifier = project.tableDesc.identifier
        outputTables.add(buildTableInfo(tableIdentifier.table, tableIdentifier.database.getOrElse(this.currentDb), OperatorType.CREATE))

      case plan: GlobalLimit =>
        val project: GlobalLimit = plan.asInstanceOf[GlobalLimit]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: LocalLimit =>
        val project: LocalLimit = plan.asInstanceOf[LocalLimit]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: With =>
        val project: With = plan.asInstanceOf[With]
        project.cteRelations.foreach(cte => {
          tmpTables.add(buildTableInfo(cte._1, "temp", OperatorType.READ))
          resolveLogic(cte._2, inputTables, outputTables, tmpTables)
        })
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: Sort =>
        val project: Sort = plan.asInstanceOf[Sort]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case ignore: SetCommand =>
        print(ignore.toString())

      case ignore: AddJarCommand =>
        print(ignore.toString())
      case ignore: CreateFunctionCommand =>
        print(ignore.toString())

      case ignore: SetDatabaseCommand =>
        print(ignore.toString())

      case ignore: OneRowRelation =>
        print(ignore.toString())

      case ignore: DropFunctionCommand =>
        print(ignore.toString())
      case plan: AlterTableAddPartitionCommand =>
        val project: AlterTableAddPartitionCommand = plan.asInstanceOf[AlterTableAddPartitionCommand]

        outputTables.add(buildTableInfo(project.tableName.table, project.tableName.database.getOrElse(this.currentDb), OperatorType.ALTER))

      case plan: AlterTableDropPartitionCommand =>
        val project: AlterTableDropPartitionCommand = plan.asInstanceOf[AlterTableDropPartitionCommand]
        outputTables.add(buildTableInfo(project.tableName.table, project.tableName.database.getOrElse(this.currentDb), OperatorType.ALTER))

      case plan: AlterTableAddColumnsCommand =>
        val project: AlterTableAddColumnsCommand = plan.asInstanceOf[AlterTableAddColumnsCommand]
        outputTables.add(buildTableInfo(project.table.table, project.table.database.getOrElse(this.currentDb), OperatorType.ALTER))


      case plan: CreateTableLikeCommand =>
        val project: CreateTableLikeCommand = plan.asInstanceOf[CreateTableLikeCommand]
        inputTables.add(buildTableInfo(project.sourceTable.table, project.sourceTable.database.getOrElse(this.currentDb), OperatorType.READ))
        outputTables.add(buildTableInfo(project.targetTable.table, project.targetTable.database.getOrElse(this.currentDb), OperatorType.CREATE))


      case plan: DropTableCommand =>
        val project: DropTableCommand = plan.asInstanceOf[DropTableCommand]
        outputTables.add(buildTableInfo(project.tableName.table, project.tableName.database.getOrElse(this.currentDb), OperatorType.DROP))


      case plan: AlterTableRecoverPartitionsCommand =>
        val project: AlterTableRecoverPartitionsCommand = plan.asInstanceOf[AlterTableRecoverPartitionsCommand]
        outputTables.add(buildTableInfo(project.tableName.table, project.tableName.database.getOrElse(this.currentDb), OperatorType.ALTER))
      case plan: GroupingSets =>
        val project: GroupingSets = plan.asInstanceOf[GroupingSets]
        resolveLogic(project.child, inputTables, outputTables, tmpTables)

      case plan: CreateDatabaseCommand =>
        val project: CreateDatabaseCommand = plan.asInstanceOf[CreateDatabaseCommand]
        inputTables.add(new TableInfo(project.databaseName, OperatorType.CREATE))

      case plan: DropDatabaseCommand =>
        val project: DropDatabaseCommand = plan.asInstanceOf[DropDatabaseCommand]
        inputTables.add(new TableInfo(project.databaseName, OperatorType.DROP))

      case plan: AlterDatabasePropertiesCommand =>
        val project: AlterDatabasePropertiesCommand = plan.asInstanceOf[AlterDatabasePropertiesCommand]
        inputTables.add(new TableInfo(project.databaseName, OperatorType.ALTER))

      case plan: ShowCreateTableCommand =>
        val project: ShowCreateTableCommand = plan.asInstanceOf[ShowCreateTableCommand]
        outputTables.add(buildTableInfo(project.table.table, project.table.database.getOrElse(this.currentDb), OperatorType.READ))

      case plan: RefreshTable =>
        val project: RefreshTable = plan.asInstanceOf[RefreshTable]
        inputTables.add(buildTableInfo(project.tableIdent.table, project.tableIdent.database.getOrElse(this.currentDb), OperatorType.READ))

      case `plan` => {
        throw new RuntimeException("******child plan******:\n" + plan.getClass.getName + "\n" + plan)
      }
    }
  }

  override protected def parseInternal(sqlText: String): (JSet[TableInfo], JSet[TableInfo], JSet[TableInfo]) = {
    val parser = new SparkSqlParser(new SQLConf)

    val logicalPlan: LogicalPlan = parser.parsePlan(sqlText)
    logicalPlan match {
      case command: SetDatabaseCommand =>
        this.currentDb = command.databaseName
        return Tuple3(new JSet[TableInfo](0), new JSet[TableInfo](0), new JSet[TableInfo](0))
      case _ =>
    }
    this.resolveLogicPlan(logicalPlan)
  }
}
