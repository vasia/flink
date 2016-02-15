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
package org.apache.flink.api.table.runtime

import java.lang.Iterable
import org.apache.flink.util.Collector
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.runtime.aggregate.Aggregate
import org.apache.flink.api.common.functions.GroupCombineFunction

/**
 * A combinable AggregateFunction.
 * It is used for all aggregates except AVG. 
 *
 * @param aggregates SQL aggregate functions.
 * @param fields The grouped keys' indices in the input.
 * @param groupingKeys The grouping keys' positions.
 */
class CombinableAggregateFunction(
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val fields: Array[Int],
    private val groupingKeys: Array[Int]) extends
    AggregateFunction(
        aggregates,
        fields,
        groupingKeys)
    with GroupCombineFunction[Row, Row] {

  override def combine(records: Iterable[Row], out: Collector[Row]): Unit = {
    reduce(records, out)
  }
}
