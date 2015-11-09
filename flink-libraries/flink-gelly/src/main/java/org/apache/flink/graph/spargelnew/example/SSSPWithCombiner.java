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

package org.apache.flink.graph.spargelnew.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.SingleSourceShortestPathsData;
import org.apache.flink.graph.spargelnew.ComputeFunction;
import org.apache.flink.graph.spargelnew.MessageCombiner;
import org.apache.flink.graph.spargelnew.MessageIterator;

/**
 * This example shows how to use the new vertex-centric iteration model.
 */
public class SSSPWithCombiner implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

		Graph<Long, Double, Double> graph = Graph.fromDataSet(edges, new InitVertices(), env);

		// Execute the vertex-centric iteration
		Graph<Long, Double, Double> result = graph.runMessagePassingIteration(
				new SSSPComputeFunction(srcVertexId), new SSSPCombiner(), 
				maxIterations, Double.POSITIVE_INFINITY);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();

		// emit result
		if (fileOutput) {
			singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			JobExecutionResult jobRes = env.execute("Single Source Shortest Paths Example");
			System.out.println("Execution time: " + jobRes.getNetRuntime());
		} else {
			singleSourceShortestPaths.print();
//			singleSourceShortestPaths.writeAsCsv("out", "\n", ",");
//			System.out.println(env.getExecutionPlan());
		}

	}

	// --------------------------------------------------------------------------------------------
	//  Single Source Shortest Path UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class InitVertices implements MapFunction<Long, Double> {

		public Double map(Long id) { return Double.POSITIVE_INFINITY; }
	}

	/**
	 * The compute function for SSSP
	 */
	@SuppressWarnings("serial")
	public static final class SSSPComputeFunction extends ComputeFunction<Long, Double, Double, Double> {

		private final long srcId;

		public SSSPComputeFunction(long src) {
			this.srcId = src;
		}

		public void compute(Vertex<Long, Double> vertex, MessageIterator<Double> messages) {

			double minDistance = (vertex.getId().equals(srcId)) ? 0d : Double.POSITIVE_INFINITY;

			for (Double msg : messages) {
				minDistance = Math.min(minDistance, msg);
			}

			if (minDistance < vertex.getValue()) {
				setNewVertexValue(minDistance);
				for (Edge<Long, Double> e: getEdges()) {
					sendMessageTo(e.getTarget(), minDistance + e.getValue());
				}
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class SSSPCombiner extends MessageCombiner<Long, Double> {

		public void combineMessages(MessageIterator<Double> messages) {

			double minMessage = Double.POSITIVE_INFINITY;
			for (Double msg: messages) {
				minMessage = Math.min(minMessage, msg);
			}
			sendCombinedMessage(minMessage);
		}
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static Long srcVertexId = 1l;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static int maxIterations = 5;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 4) {
				System.err.println("Usage: SSSPCompute <source vertex id>" +
						" <input edges path> <output path> <num iterations>");
				return false;
			}

			fileOutput = true;
			srcVertexId = Long.parseLong(args[0]);
			edgesInputPath = args[1];
			outputPath = args[2];
			maxIterations = Integer.parseInt(args[3]);
		} else {
				System.out.println("Executing Single Source Shortest Paths example "
						+ "with default parameters and built-in default data.");
				System.out.println("  Provide parameters to read input data from files.");
				System.out.println("  See the documentation for the correct format of input files.");
				System.out.println("Usage: SingleSourceShortestPaths <source vertex id>" +
						" <input edges path> <output path> <num iterations>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n")
					.fieldDelimiter(" ")
					.ignoreComments("%")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long,Long>, Edge<Long, Double>>() {

						public Edge<Long, Double> map(Tuple2<Long, Long> value) {
							return new Edge<Long, Double>(value.f0, value.f1, 1.0);
						}
					});
		} else {
			return SingleSourceShortestPathsData.getDefaultEdgeDataSet(env);
		}
	}

	@Override
	public String getDescription() {
		return "Vertex-centric Single Source Shortest Paths";
	}
}