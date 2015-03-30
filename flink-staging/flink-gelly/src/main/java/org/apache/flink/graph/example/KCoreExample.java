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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.KCoreData;
import org.apache.flink.types.NullValue;

/**
 * 
 * This example computes the k-core of the input graph, i.e.
 * the subgraph in which all vertices have degree of at least k.
 * 
 * To find the the k-core,we iteratively filter out vertices with degree
 * less than k.

 * The algorithm stops when there are no more vertex removals. At the end of
 * the execution, the remaining graph represents the k-core. It is possible that
 * the result is an empty graph.
 *
 */
public class KCoreExample implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/** create the undirected graph **/
		Graph<Long, Long, NullValue> undirectedGraph = Graph.fromDataSet(getEdgesDataSet(env), env).getUndirected()
				.mapVertices(
						new MapFunction<Vertex<Long, NullValue>, Long>() {

							public Long map(Vertex<Long, NullValue> vertex) {
								return 0l;
							}
				});

		Graph<Long, Long, NullValue> kCoreGraph = undirectedGraph;
		boolean vertexRemoved = true; // termination flag

		while(vertexRemoved) {
			/** attach the vertex out-degrees as the vertex values **/
			DataSet<Tuple2<Long, Long>> verticesWithDegrees = kCoreGraph.outDegrees();
	
			Graph<Long, Long, NullValue> newKCoreGraph = kCoreGraph.joinWithVertices(verticesWithDegrees, 
					new MapFunction<Tuple2<Long, Long>, Long>() {
	
						public Long map(Tuple2<Long, Long> value) throws Exception {
							return value.f1;
						}
	
			/** filter out the vertices with degree < k **/
			}).filterOnVertices(new FilterFunction<Vertex<Long,Long>>() {
	
				public boolean filter(Vertex<Long, Long> vertex) throws Exception {
					return (vertex.getValue() >= K);
				}
			});

			/** check whether we need to run another iteration **/
			long previousNumVertices = kCoreGraph.numberOfVertices();
			long currentNumVertices = newKCoreGraph.numberOfVertices();

			if (currentNumVertices > 0 && (currentNumVertices < previousNumVertices)) {
				// a vertex was removed --> continue iterating
				vertexRemoved = true;
			}
			else {
				vertexRemoved = false;
			}
			kCoreGraph = newKCoreGraph;
		}

		// Emit results
		if(fileOutput) {
			kCoreGraph.getVertexIds().writeAsCsv(outputPath);
		} else {
			kCoreGraph.getVertexIds().print();
		}

		env.execute();
	}


	@Override
	public String getDescription() {
		return "k-core Example";
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static int K = 3;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage: KCoreExample <input edges> <K-value> <output>");
				return false;
			}

			fileOutput = true;
			edgesInputPath = args[0];
			outputPath = args[2];
			K = Integer.parseInt(args[1]);
		} else {
			System.out.println("Executing K-Core example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("Usage: KCoreExample <input edges> <K-value> <output>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n").fieldDelimiter("\t")
					.types(Long.class, Long.class).map(
							new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

								public Edge<Long, NullValue> map(Tuple2<Long, Long> value) {
									return new Edge<Long, NullValue>(value.f0, value.f1, 
											NullValue.getInstance());
								}
					});
		} else {
			return KCoreData.getKCoreEdges(env).map(
					new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

						public Edge<Long, NullValue> map(Tuple2<Long, Long> value) {
							return new Edge<Long, NullValue>(value.f0, value.f1, 
									NullValue.getInstance());
						}
			});
		}
	}
}
