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
import org.apache.flink.graph.example.utils.ConnectedComponentsDefaultData;
import org.apache.flink.graph.spargelnew.ComputeFunction;
import org.apache.flink.graph.spargelnew.MessageCombiner;
import org.apache.flink.graph.spargelnew.MessageIterator;
import org.apache.flink.types.NullValue;

/**
 * This example shows how to use the new vertex-centric iteration model.
 */
public class PregelCCWithCombiner implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, NullValue>> edges = getEdgesDataSet(env);

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(edges, new InitVertices(), env);

		// Execute the vertex-centric iteration
		Graph<Long, Long, NullValue> result = graph.getUndirected().runMessagePassingIteration(
				new CCComputeFunction(), new CCCombiner(), maxIterations);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Long>> cc = result.getVertices();

		// emit result
		if (fileOutput) {
			cc.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			JobExecutionResult jobRes = env.execute("Connected Components Pregel w/ Combiner");
			System.out.println("Execution time: " + jobRes.getNetRuntime());
		} else {
			cc.print();
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Single Source Shortest Path UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class InitVertices implements MapFunction<Long, Long> {
		public Long map(Long id) { return id; }
	}

	/**
	 * The compute function for CC
	 */
	@SuppressWarnings("serial")
	public static final class CCComputeFunction extends ComputeFunction<Long, Long, NullValue, Long> {

		public void compute(Vertex<Long, Long> vertex, MessageIterator<Long> messages) {

			long currentComponent = vertex.getValue();

			for (Long msg : messages) {
				currentComponent = Math.min(currentComponent, msg);
			}

			if ((getSuperstepNumber() == 1) || (currentComponent < vertex.getValue())) {
				setNewVertexValue(currentComponent);
				for (Edge<Long, NullValue> edge: getEdges()) {
					sendMessageTo(edge.getTarget(), currentComponent);
				}
			}
		}
		
	}

	@SuppressWarnings("serial")
	public static final class CCCombiner extends MessageCombiner<Long, Long> {

		public void combineMessages(MessageIterator<Long> messages) {

			long minMessage = Long.MAX_VALUE;
			for (Long msg: messages) {
				minMessage = Math.min(minMessage, msg);
			}
			sendCombinedMessage(minMessage);
		}
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static int maxIterations = 5;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage: PregelCC " +
						" <input edges path> <output path> <num iterations>");
				return false;
			}

			fileOutput = true;
			edgesInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
		} else {
				System.out.println("Executing Pregel CC "
						+ "with default parameters and built-in default data.");
				System.out.println("  Provide parameters to read input data from files.");
				System.out.println("  See the documentation for the correct format of input files.");
				System.out.println("Usage: PregelCC " +
						" <input edges path> <output path> <num iterations>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n")
					.fieldDelimiter("\t")
					.ignoreComments("#")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
						@Override
						public Edge<Long, NullValue> map(Tuple2<Long, Long> value) throws Exception {
							return new Edge<Long, NullValue>(value.f0, value.f1, NullValue.getInstance());
						}
					});
		} else {
			return ConnectedComponentsDefaultData.getDefaultEdgeDataSet(env);
		}
	}

	@Override
	public String getDescription() {
		return "Vertex-centric Connected Components";
	}
}