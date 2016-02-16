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

package org.apache.flink.api.table.plan.rules.logical

import org.apache.calcite.plan.{Convention, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.flink.api.table.plan.nodes.logical.{FlinkAggregate, FlinkConvention}
import org.apache.calcite.plan.RelOptRuleCall
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

class FlinkAggregateRule
  extends ConverterRule(
      classOf[LogicalAggregate],
      Convention.NONE,
      FlinkConvention.INSTANCE,
      "FlinkAggregateRule")
  {

  // do no match if there are aggregates on overlapping fields
    override def matches(call: RelOptRuleCall): Boolean = {
      var overlap = true
      if (super.matches(call)) {
        val aggFields = scala.collection.mutable.Set.empty[Integer]
        val aggCall = call.rel(0).asInstanceOf[LogicalAggregate].getAggCallList
        
        // go over aggCalls and check if calls access the same input fields
        aggCall.foreach(agg => agg.getArgList.foreach(arg => {
          if (aggFields.contains(arg)) {
            // found an aggregate that accesses the same field
            overlap = false
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

    def convert(rel: RelNode): RelNode = {
      val agg: LogicalAggregate = rel.asInstanceOf[LogicalAggregate]
      val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConvention.INSTANCE)
      val convInput: RelNode = RelOptRule.convert(agg.getInput, FlinkConvention.INSTANCE)

      new FlinkAggregate(
        rel.getCluster,
        traitSet,
        convInput,
        agg.indicator,
        agg.getGroupSet,
        agg.getGroupSets,
        agg.getAggCallList)
    }
  }

object FlinkAggregateRule {
  val INSTANCE: RelOptRule = new FlinkAggregateRule
}
