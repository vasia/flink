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

package org.apache.flink.api.table.plan.rules

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.rel.logical.LogicalAggregate
import scala.collection.JavaConversions._
import org.apache.calcite.rex.RexInputRef
import scala.collection.mutable.ArrayBuffer
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.rel.core.RelFactories
import java.util.ArrayList
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import scala.collection.mutable.ListBuffer

/**
 * This rule injects a Project operator before an aggregation
 * in order to make it combinable, i.e. convert the GroupReduceFunction
 * input type to contain distinct fields for each aggregate
 * so that the GroupCombineFunction can be applied.
 */
class AggregateCombinableRule(
    operand: RelOptRuleOperand,
    relBuilderFactory: RelBuilderFactory) extends
    RelOptRule(
        operand,
        relBuilderFactory,
        null) {

  // Only match if overlapping fields in aggregates
  override def matches(call: RelOptRuleCall): Boolean = {
    var overlap = false
    if (super.matches(call)) {
      val aggFields = scala.collection.mutable.Set.empty[Integer]
      val aggCall = call.rel(0).asInstanceOf[LogicalAggregate].getAggCallList
        
      // go over aggCalls and check if calls access the same input fields
      aggCall.foreach(agg => agg.getArgList.foreach(arg => {
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
    else {
      false
    }
  }

  override def onMatch(call: RelOptRuleCall) = {
    val aggregate = call.rel(0).asInstanceOf[LogicalAggregate]
    val relBuilder = call.builder
    relBuilder.push(aggregate.getInput)
    val aggFields = aggregate.getRowType.getFieldList
    val fieldNames = aggregate.getRowType.getFieldNames
    val groupSet = aggregate.getGroupSet //grouping
    val refs = ArrayBuffer.empty[RexInputRef]
    val newAggCallList = new ArrayList[AggregateCall]

    var keyMappings = List.empty[Integer]
    groupSet.foreach(groupKey => {
       // add them in the beginning of the projection
      refs.add(relBuilder.field(groupKey))
     // create a mapping from the old key positions to the new ones
    // in order to create the new groupingSets
      keyMappings = (refs.length - 1)::keyMappings
    })

    val newArgs = new ArrayList[Integer]
    aggregate.getAggCallList.foreach(aggCall => {
      newArgs.clear
      for (i <- 0 until aggCall.getArgList.length) {
        refs.add(relBuilder.field(aggCall.getArgList.get(i)))
        newArgs.add(i, refs.length - 1)
      }
      // create a new aggCall that points to the new place in the
      // field list, so that no aggregate accesses the same field
      // otherwise, the rule will match again
      newAggCallList.add(aggCall.copy(newArgs))
    })
    // project each field once for each agg
    relBuilder.project(refs, fieldNames)
    
    val groupFields = keyMappings.map(key => relBuilder.field(key))
    val groupKey = relBuilder.groupKey(groupFields)

    relBuilder.aggregate(groupKey, newAggCallList)
    call.transformTo(relBuilder.build)
  }
}

object AggregateCombinableRule {
  val INSTANCE = new AggregateCombinableRule(
      RelOptRule.operand(classOf[LogicalAggregate], RelOptRule.any), RelFactories.LOGICAL_BUILDER)
}