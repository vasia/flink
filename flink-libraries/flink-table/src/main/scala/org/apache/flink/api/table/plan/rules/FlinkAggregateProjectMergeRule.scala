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
import org.apache.calcite.rel.rules.AggregateProjectMergeRule
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.Project
import org.apache.flink.api.table.plan.nodes.logical.FlinkAggregate

/**
 * This extends AggregateProjectMergeRule and override
 * its match method to only match logical aggregates
 * and not FlinkAggregates, so that it is not applied
 * after AggregateCombinableRule.
 */
class FlinkAggregateProjectMergeRule(
    aggregateClass: Class[_ <: Aggregate],
    projectClass: Class[_ <: Project],
    relBuilderFactory: RelBuilderFactory) extends
    AggregateProjectMergeRule(
        aggregateClass,
        projectClass,
        relBuilderFactory) {

  // Only allow a match for logical aggregates,
  // not when the call is a FlinkAggregate.
  // This way it won't match after the AggregateCombinableRule.
  override def matches(call: RelOptRuleCall): Boolean = {
    if(call.rel(0).isInstanceOf[FlinkAggregate]) {
      false
    }
    else {
      true
    }
  }
}

object FlinkAggregateProjectMergeRule {
  val INSTANCE = new FlinkAggregateProjectMergeRule(
      classOf[Aggregate],
      classOf[Project],
      RelFactories.LOGICAL_BUILDER)
}