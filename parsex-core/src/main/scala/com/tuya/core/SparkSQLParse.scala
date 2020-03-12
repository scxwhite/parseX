package com.tuya.core

import java.util.{HashSet => JSet}

import com.tuya.core.enums.OperatorType
import com.tuya.core.model.TableInfo
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConversions._

class SparkSQLParse extends AbstractSqlParse {

  val columnsSet = new JSet[String]()


  private[this] def resolveLogicPlan(plan: LogicalPlan, currentDB: String) = {
    val inputTables = new JSet[TableInfo]()
    val outputTables = new JSet[TableInfo]()
    val tmpTables = new JSet[TableInfo]()
    resolveLogic(plan, currentDB, inputTables, outputTables, tmpTables)
    Tuple4(inputTables, outputTables, tmpTables, currentDB)
  }


  private[this] def resolveLogic(plan: LogicalPlan, currentDB: String, inputTables: JSet[TableInfo], outputTables: JSet[TableInfo], tmpTables: JSet[TableInfo]): Unit = {
    plan match {

      case plan: Project =>
        val project: Project = plan.asInstanceOf[Project]
        project.projectList.foreach {
          case field@(_: UnresolvedAttribute) =>
            columnsSet.add(field.name)
          case field@(_: UnresolvedStar) =>
            columnsSet.add(field.asInstanceOf[UnresolvedStar].toString())
          case _ =>
        }
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: Union =>
        val project: Union = plan.asInstanceOf[Union]
        for (child <- project.children) {
          resolveLogic(child, currentDB, inputTables, outputTables, tmpTables)
        }
      case plan: Join =>
        val project: Join = plan.asInstanceOf[Join]
        resolveLogic(project.left, currentDB, inputTables, outputTables, tmpTables)
        resolveLogic(project.right, currentDB, inputTables, outputTables, tmpTables)

      case plan: Aggregate =>
        val project: Aggregate = plan.asInstanceOf[Aggregate]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: Filter =>
        val project: Filter = plan.asInstanceOf[Filter]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: Generate =>
        val project: Generate = plan.asInstanceOf[Generate]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: RepartitionByExpression =>
        val project: RepartitionByExpression = plan.asInstanceOf[RepartitionByExpression]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: SerializeFromObject =>
        val project: SerializeFromObject = plan.asInstanceOf[SerializeFromObject]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: MapPartitions =>
        val project: MapPartitions = plan.asInstanceOf[MapPartitions]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: DeserializeToObject =>
        val project: DeserializeToObject = plan.asInstanceOf[DeserializeToObject]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: Repartition =>
        val project: Repartition = plan.asInstanceOf[Repartition]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: Deduplicate =>
        val project: Deduplicate = plan.asInstanceOf[Deduplicate]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: Window =>
        val project: Window = plan.asInstanceOf[Window]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: MapElements =>
        val project: MapElements = plan.asInstanceOf[MapElements]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: TypedFilter =>
        val project: TypedFilter = plan.asInstanceOf[TypedFilter]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: Distinct =>
        val project: Distinct = plan.asInstanceOf[Distinct]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: SubqueryAlias =>
        val project: SubqueryAlias = plan.asInstanceOf[SubqueryAlias]
        val childInputTables = new JSet[TableInfo]()
        val childOutputTables = new JSet[TableInfo]()

        resolveLogic(project.child, currentDB, childInputTables, childOutputTables, tmpTables)
        if (childInputTables.size() > 0) {
          for (table <- childInputTables) inputTables.add(table)
        } else {
          inputTables.add(new TableInfo(project.alias, currentDB, OperatorType.READ, columnsSet))
        }

      case plan: UnresolvedCatalogRelation =>
        val project: UnresolvedCatalogRelation = plan.asInstanceOf[UnresolvedCatalogRelation]
        val identifier: TableIdentifier = project.tableMeta.identifier
        inputTables.add(new TableInfo(identifier.table, identifier.database.getOrElse(currentDB), OperatorType.READ, columnsSet))

      case plan: UnresolvedRelation =>
        val project: UnresolvedRelation = plan.asInstanceOf[UnresolvedRelation]
        val TableInfo = new TableInfo(project.tableIdentifier.table, project.tableIdentifier.database.getOrElse(currentDB), OperatorType.READ, columnsSet)
        inputTables.add(TableInfo)

      case plan: InsertIntoTable =>
        val project: InsertIntoTable = plan.asInstanceOf[InsertIntoTable]

        plan.table match {
          case relation: UnresolvedRelation =>
            val table: TableIdentifier = relation.tableIdentifier
            outputTables.add(new TableInfo(table.table, table.database.getOrElse(currentDB), OperatorType.WRITE, columnsSet))
          case _ =>
            //resolveLogic(project.table, currentDB, outputTables, inputTables, tmpTables)
            throw new RuntimeException("无法解析的插入逻辑语法树:" + plan.table)
        }

        resolveLogic(project.query, currentDB, inputTables, outputTables, tmpTables)

      case plan: CreateTable =>
        val project: CreateTable = plan.asInstanceOf[CreateTable]
        if (project.query.isDefined) {
          resolveLogic(project.query.get, currentDB, inputTables, outputTables, tmpTables)
        }
        project.tableDesc.schema.fields.foreach(field => {
          columnsSet.add(field.name)
        })
        val tableIdentifier: TableIdentifier = project.tableDesc.identifier
        outputTables.add(new TableInfo(tableIdentifier.table, tableIdentifier.database.getOrElse(currentDB), OperatorType.CREATE, columnsSet))

      case plan: GlobalLimit =>
        val project: GlobalLimit = plan.asInstanceOf[GlobalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: LocalLimit =>
        val project: LocalLimit = plan.asInstanceOf[LocalLimit]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: With =>
        val project: With = plan.asInstanceOf[With]
        project.cteRelations.foreach(cte => {
          tmpTables.add(new TableInfo(cte._1, "temp", OperatorType.READ, columnsSet))
          resolveLogic(cte._2, currentDB, inputTables, outputTables, tmpTables)
        })
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

      case plan: Sort =>
        val project: Sort = plan.asInstanceOf[Sort]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)

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

        outputTables.add(new TableInfo(project.tableName.table, project.tableName.database.getOrElse(currentDB), OperatorType.ALTER, columnsSet))

      case plan: AlterTableDropPartitionCommand =>
        val project: AlterTableDropPartitionCommand = plan.asInstanceOf[AlterTableDropPartitionCommand]
        outputTables.add(new TableInfo(project.tableName.table, project.tableName.database.getOrElse(currentDB), OperatorType.ALTER, columnsSet))

      case plan: AlterTableAddColumnsCommand =>
        val project: AlterTableAddColumnsCommand = plan.asInstanceOf[AlterTableAddColumnsCommand]
        outputTables.add(new TableInfo(project.table.table, project.table.database.getOrElse(currentDB), OperatorType.ALTER, columnsSet))


      case plan: CreateTableLikeCommand =>
        val project: CreateTableLikeCommand = plan.asInstanceOf[CreateTableLikeCommand]
        inputTables.add(new TableInfo(project.sourceTable.table, project.sourceTable.database.getOrElse(currentDB), OperatorType.READ, columnsSet))
        outputTables.add(new TableInfo(project.targetTable.table, project.targetTable.database.getOrElse(currentDB), OperatorType.CREATE, columnsSet))


      case plan: DropTableCommand =>
        val project: DropTableCommand = plan.asInstanceOf[DropTableCommand]
        outputTables.add(new TableInfo(project.tableName.table, project.tableName.database.getOrElse(currentDB), OperatorType.DROP, columnsSet))


      case plan: AlterTableRecoverPartitionsCommand =>
        val project: AlterTableRecoverPartitionsCommand = plan.asInstanceOf[AlterTableRecoverPartitionsCommand]
        outputTables.add(new TableInfo(project.tableName.table, project.tableName.database.getOrElse(currentDB), OperatorType.ALTER, columnsSet))
      case plan: GroupingSets =>
        val project: GroupingSets = plan.asInstanceOf[GroupingSets]
        resolveLogic(project.child, currentDB, inputTables, outputTables, tmpTables)


      case `plan` => {
        throw new RuntimeException("******child plan******:\n" + plan.getClass.getName + "\n" + plan)
      }
    }
  }

  override protected def parseInternal(sqlText: String, currentDb: String): (JSet[TableInfo], JSet[TableInfo], JSet[TableInfo], String) = {
    val parser = new SparkSqlParser(new SQLConf)

    val logicalPlan: LogicalPlan = parser.parsePlan(sqlText)
    logicalPlan match {
      case command: SetDatabaseCommand =>
        return Tuple4(new JSet[TableInfo](0), new JSet[TableInfo](0), new JSet[TableInfo](0), command.databaseName)
      case _ =>
    }
    this.resolveLogicPlan(logicalPlan, currentDb)
  }
}
