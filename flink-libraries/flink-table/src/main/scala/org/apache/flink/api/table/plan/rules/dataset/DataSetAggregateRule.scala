/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.table.plan.rules.dataset

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetConvention, DataSetGroupReduce}
import org.apache.flink.api.table.plan.nodes.logical.{FlinkAggregate, FlinkConvention}
import scala.collection.JavaConversions._
import org.apache.flink.api.table.runtime.aggregate.AggregateFactory
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.calcite.sql.SqlAggFunction
import scala.collection.mutable.Buffer
import org.apache.calcite.sql.fun.SqlSumAggFunction
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction
import org.apache.calcite.sql.fun.SqlCountAggFunction
import org.apache.flink.api.table.plan.nodes.dataset.DataSetCombinableGroupReduce
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.table.runtime.MapRunner
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.RexInputRef
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.table.plan.TypeConverter
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.rel.core.RelFactories

class DataSetAggregateRule
  extends ConverterRule(
    classOf[FlinkAggregate],
    FlinkConvention.INSTANCE,
    DataSetConvention.INSTANCE,
    "DataSetAggregateRule")
{

  def convert(rel: RelNode): RelNode = {
    val agg: FlinkAggregate = rel.asInstanceOf[FlinkAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, DataSetConvention.INSTANCE)
    val grouping = agg.getGroupSet.toArray
    val inputType = agg.getInput.getRowType
    val aggCalls = agg.getAggCallList
    val aggregations = aggCalls.map(aggCall => aggCall.getAggregation)
    val newAggCallList = new ArrayList[AggregateCall]
    val newGrouping = ArrayBuffer.empty[Int]
    val projectionTypes = ArrayBuffer.empty[TypeInformation[Any]]

//    val relBuilder = RelFactories.LOGICAL_BUILDER.create(agg.getCluster, null)

    var projectionFun: ((TableConfig, TypeInformation[Any], TypeInformation[Any]) =>
      MapFunction[Any, Any]) = null
    var projection = false

    // check if combinable (min, max, count, sum)
    if (allCombinable(aggregations)) {
      // if there are overlapping fields => generate a projection
      if (overlappingFields(aggCalls.toList)) {
        projectionFun = getProjectionFunction(agg, newAggCallList, newGrouping)
        //TODO: somehow get the projection output type, i.e. the new aggregation's input type
        projection = true

        // create a new aggregation
        val aggregateFunction = AggregateFactory.createAggregateInstance(
            newAggCallList,
            inputType,
            newGrouping.toArray,
            true)

        new DataSetCombinableGroupReduce(
          rel.getCluster,
          traitSet,
          convInput,
          rel.getRowType,
          agg.toString,
          newGrouping.toArray,
          projection,
          projectionFun,
          aggregateFunction)
      }
      else {
        // no overlapping fields => TODO: projection only if there is a count
        val aggregateFunction = AggregateFactory.createAggregateInstance(
            agg.getAggCallList,
            inputType,
            grouping,
            true)

        new DataSetCombinableGroupReduce(
            rel.getCluster,
            traitSet,
            convInput,
            rel.getRowType,
            agg.toString,
            grouping,
            projection,
            projectionFun,
            aggregateFunction)
      }
    }
    else {
     // not combinable =>
     // add grouping fields, position keys in the input, and input type
      val aggregateFunction = AggregateFactory.createAggregateInstance(
          agg.getAggCallList,
          inputType,
          grouping,
          false)

      new DataSetGroupReduce(
        rel.getCluster,
        traitSet,
        convInput,
        rel.getRowType,
        agg.toString,
        grouping,
        aggregateFunction)
    }
  }

  /**
   * Check if all aggregation functions in the input list are combinable 
   */
  def allCombinable(aggregatesList: Buffer[SqlAggFunction]): Boolean = {
    val combinableFuns = aggregatesList.filter(aggFun => isCombinableAggFun(aggFun))
    aggregatesList.length == combinableFuns.length
  }

  def isCombinableAggFun(fun: SqlAggFunction): Boolean = {
     fun match {
       case _: SqlSumAggFunction |
            _: SqlSumEmptyIsZeroAggFunction |
            _: SqlMinMaxAggFunction |
            _: SqlCountAggFunction =>
         true
       case other =>
         false
     }
  }

  /**
   * Check if there aggregations on overlapping fields 
   */
  def overlappingFields(aggCalls: List[AggregateCall]): Boolean = {
    var overlap = false
    val aggFields = scala.collection.mutable.Set.empty[Integer]
    // go over aggCalls and check if calls access the same input fields
    aggCalls.foreach(agg => agg.getArgList.foreach(arg => {
      if (aggFields.contains(arg)) {
        // found an aggregate that accesses the same field
        overlap = true
      }
      else {
        aggFields.add(arg)
      }
    }))
    overlap
  }

  /**
   * Generate the projection function and the new aggregation grouping and arguments
   */
  def getProjectionFunction(
      agg: FlinkAggregate,
      newAggCallList: ArrayList[AggregateCall],
      groupFields: ArrayBuffer[Int]):
      ((TableConfig, TypeInformation[Any], TypeInformation[Any]) =>
      MapFunction[Any, Any]) = {

    val projectionFunc = (
        config: TableConfig,
        inputType: TypeInformation[Any],
        returnType: TypeInformation[Any]) => {

    val generator = new CodeGenerator(config, inputType)
    val projectionTypes = ArrayBuffer.empty[TypeInformation[Any]]

    // the projection return type will be
    // types(grouping keys) + types(fields (repeated if overlapping))
    val rowInputType = inputType.asInstanceOf[RowTypeInfo]
    val rexNodes = Buffer.empty[RexInputRef] // new projection expressions
    val fieldList = agg.getRowType.getFieldList
    val newArgs = new ArrayList[Integer]

    // get the grouping fields; these stay the same
    for (i <- 0 until agg.getGroupSet.length) {
      projectionTypes.add(rowInputType.getTypeAt(i))
      rexNodes.add(RexInputRef.of(i, fieldList))
      groupFields.add(i)  // FIXME: I need a mapping here or a relBuilder

    }
    agg.getAggCallList.foreach(aggCall => {
      newArgs.clear
      // add a field for each aggregate, accounting for overlaps
      for (i <- 0 until aggCall.getArgList.length) {
        rexNodes.add(RexInputRef.of(aggCall.getArgList.get(i), fieldList))
        newArgs.add(i, rexNodes.length - 1)
        projectionTypes.add(rowInputType.getTypeAt(aggCall.getArgList.get(i)))
      }

      // the new aggCallList points to the new place in the
      // field list, so that no aggregate accesses the same field
      newAggCallList.add(aggCall.copy(newArgs))
    })

    val projectRelNode = RelFactories.DEFAULT_PROJECT_FACTORY.createProject(
        agg.getInput,
        rexNodes,
        rowInputType.getFieldNames.toList) //FIXME: this might be wrong

    projectionTypes.foreach(ptype => println("type: " +ptype))
    val projectionReturnType = new RowTypeInfo(projectionTypes)
    val projection = generator.generateResultExpression(
        projectionReturnType,
        projectionReturnType.getFieldNames,
        rexNodes)

    val body =
      s"""
        |${projection.code}
        |return ${projection.resultTerm};
        |""".stripMargin

    val genFunction = generator.generateFunction(
        description,
        classOf[MapFunction[Any, Any]],
        body,
        returnType)

    new MapRunner[Any, Any](
        genFunction.name,
        genFunction.code,
        genFunction.returnType)
    }
    projectionFunc
  }

}

object DataSetAggregateRule {
  val INSTANCE: RelOptRule = new DataSetAggregateRule
}

