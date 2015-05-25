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

package org.apache.flink.graph.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;

public class Undirected implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/** create the graph **/
		Graph<Long, NullValue, Double> graph = Graph.fromDataSet(getEdgesDataSet(env), env);
		
		DataSet<Edge<Long, Double>> positiveEdges = graph.getEdges().flatMap(
				new FlatMapFunction<Edge<Long, Double>, Edge<Long, Double>>() {

					public void flatMap(Edge<Long, Double> edge,
							Collector<Edge<Long, Double>> out) throws Exception {
						if (edge.getValue() == 0) {
							out.collect(new Edge<Long, Double>(edge.getSource(), edge.getTarget(), 0.00001));
						}
						else {
							out.collect(edge);
						}
					}
		});

		positiveEdges.writeAsCsv(args[1], "\n", "\t").setParallelism(1);

		env.execute();
	}

	@Override
	public String getDescription() {
		return "Graph Metrics Example";
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static String edgesInputPath = null;

	static final int NUM_VERTICES = 100;

	static final long SEED = 9876;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 2) {
				System.err.println("Usage: GraphMetrics <input edges>");
				return false;
			}

			fileOutput = true;
			edgesInputPath = args[0];
		} else {
			System.out.println("Executing Graph Metrics example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("Usage: GraphMetrics <input edges>");
		}
		return true;
	}

	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n").fieldDelimiter("\t")
					.types(Long.class, Long.class, Double.class).map(
							new Tuple3ToEdgeMap<Long, Double>());
	}
}
