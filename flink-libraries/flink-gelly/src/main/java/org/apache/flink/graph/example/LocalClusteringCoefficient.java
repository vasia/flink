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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;

@SuppressWarnings("serial")
public class LocalClusteringCoefficient implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Vertex<Long, NullValue>> vertices = getVerticesDataSet(env);
		DataSet<Edge<Long, NullValue>> edges = getEdgesDataSet(env);
		Graph<Long, NullValue, NullValue> graph = Graph.fromDataSet(vertices, edges, env);

		// get all neighbors and attach as vertex value
		DataSet<Vertex<Long, HashSet<Long>>> verticesWithNeighbors =
				graph.groupReduceOnEdges(new EdgesFunction<Long, NullValue, Tuple2<Long, HashSet<Long>>> () {
			
					public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, NullValue>>> edges,
					 Collector<Tuple2<Long, HashSet<Long>>> out) throws Exception {
				
						HashSet<Long> neighbors = new HashSet<Long>();
						long vertexId = -1;
						for (Tuple2<Long, Edge<Long, NullValue>> edge : edges) {
							vertexId = edge.f0;
							if (edge.f0.equals(edge.f1.f0)) {
								neighbors.add(edge.f1.f1);
							}
							else {
								neighbors.add(edge.f1.f0);
							}
						}
						out.collect(new Tuple2<Long, HashSet<Long>>(vertexId, neighbors));
					}
			
				}, EdgeDirection.ALL).map(new Tuple2ToVertexMap<Long, HashSet<Long>>());

		Graph<Long, HashSet<Long>, NullValue> graphWithNeighbors =
				Graph.fromDataSet(verticesWithNeighbors, edges, env);

		DataSet<Tuple2<Long, Double>> result = graphWithNeighbors.groupReduceOnNeighbors(
				new NeighborsFunctionWithVertexValue<Long, HashSet<Long>, NullValue, Tuple2<Long, Double>>() {

					public void iterateNeighbors(
							Vertex<Long, HashSet<Long>> vertex,
							Iterable<Tuple2<Edge<Long, NullValue>, Vertex<Long, HashSet<Long>>>> neighbors,
							Collector<Tuple2<Long, Double>> out) {
						long numLinks = 0;
						Vertex<Long, HashSet<Long>> currentNeighbor;
						for (Tuple2<Edge<Long, NullValue>, Vertex<Long, HashSet<Long>>> n: neighbors) {
							currentNeighbor = n.f1;
							for (long nn: currentNeighbor.f1) {
								if (vertex.getValue().contains(nn)) {
									numLinks++;
								}
							}
						}
						long setSize = vertex.getValue().size();
						if (setSize < 2) {
							out.collect(new Tuple2<Long, Double>(vertex.getId(), 0.0));
						}
						else {
							out.collect(new Tuple2<Long, Double>(
									vertex.getId(), (double)numLinks / (double)(setSize * (setSize - 1))));	
						}
					}
					
				},
				EdgeDirection.ALL);

		result.print();

	}

	@Override
	public String getDescription() {
		return "Local clustering coefficient";
	}


	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static String verticesInputPath = null;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length == 3) {
				fileOutput = true;
				verticesInputPath = args[0];
				edgesInputPath = args[1];
				outputPath = args[2];
			} else {
				System.out.println("Executing Euclidean Graph Weighing example with default parameters and built-in default data.");
				System.out.println("Provide parameters to read input data from files.");
				System.out.println("See the documentation for the correct format of input files.");
				System.err.println("Usage: EuclideanGraphWeighing <input vertices path> <input edges path>" +
						" <output path>");
				return false;
			}
		}
		return true;
	}

	private static DataSet<Vertex<Long, NullValue>> getVerticesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(verticesInputPath)
					.lineDelimiter("\n")
					.types(Long.class)
					.map(new MapFunction<Tuple1<Long>, Vertex<Long, NullValue>>() {

						@Override
						public Vertex<Long, NullValue> map(Tuple1<Long> value) throws Exception {
							return new Vertex<>(value.f0, NullValue.getInstance());
						}
					});
		} else {
			return env.fromElements(1l, 2l, 3l, 4l).map(
					new MapFunction<Long, Vertex<Long, NullValue>>() {

						public Vertex<Long, NullValue> map(Long value) {
							return new Vertex<>(value, NullValue.getInstance());
						}
			});
		}
	}

	private static DataSet<Edge<Long, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

						@Override
						public Edge<Long, NullValue> map(Tuple2<Long, Long> tuple2) throws Exception {
							return new Edge<Long, NullValue>(tuple2.f0, tuple2.f1, NullValue.getInstance());
						}
					});
		} else {
			return env.fromElements(new Tuple2<Long, Long>(1l, 2l), new Tuple2<Long, Long>(1l, 3l),
					new Tuple2<Long, Long>(1l, 4l), new Tuple2<Long, Long>(3l, 4l)).map(
							new MapFunction<Tuple2<Long,Long>, Edge<Long, NullValue>>() {

								public Edge<Long, NullValue> map(Tuple2<Long, Long> value) {
									return new Edge<>(value.f0, value.f1, NullValue.getInstance());
								}
					});
		}
	}
}
