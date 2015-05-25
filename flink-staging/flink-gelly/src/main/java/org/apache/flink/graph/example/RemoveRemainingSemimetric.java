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

import java.util.List;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.SSSPSemimetric;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.util.Collector;

public class RemoveRemainingSemimetric implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/** create the graph **/

		DataSet<Tuple3<Long, Long, Tuple2<Double, Boolean>>> inputEdges = 
				env.readCsvFile(args[0]).fieldDelimiter("\t")
				.types(Long.class, Long.class, Double.class, String.class)
				.map(new MapFunction<Tuple4<Long, Long, Double, String>, Tuple3<Long, Long, Tuple2<Double, Boolean>>>() {

					public Tuple3<Long, Long, Tuple2<Double, Boolean>> map(
							Tuple4<Long, Long, Double, String> value) {
						return new Tuple3<Long, Long, Tuple2<Double, Boolean>>(
								value.f0, value.f1,
								new Tuple2<Double, Boolean>(value.f2, Boolean.parseBoolean(value.f3)));
					}
				});

		DataSet<Edge<Long, Tuple2<Double, Boolean>>> newEdges = inputEdges.map(new Tuple3ToEdgeMap<Long, Tuple2<Double, Boolean>>());
		List<Edge<Long, Tuple2<Double, Boolean>>> edgeList = null;

		int i=0;

		while (i < 1000) {
			Graph<Long, Double, Tuple2<Double, Boolean>> graph = null;
			if (i == 0) {
				graph = Graph.fromDataSet(newEdges,	new MapFunction<Long, Double>() {
							public Double map(Long value) {
								return Double.POSITIVE_INFINITY;
							}
				}, env);
			}
			else {
				graph = Graph.fromCollection(edgeList, new MapFunction<Long, Double>() {
					public Double map(Long value) {
						return Double.POSITIVE_INFINITY;
					}
				}, env);
			}

			//TODO: collect source with max weight and
			// (1) check that maxWeight > 0, otherwise break;
			// (2) give maxWeight as a parameter to stop SSSP early
			DataSet<Vertex<Long, Double>> initialVertices = findNewSource(graph);
			
			Graph<Long, Double, Tuple2<Double, Boolean>> nextGraphInput = Graph.fromDataSet(
					initialVertices, graph.getEdges(), env);
	
			// compute sssp
			Graph<Long, Double, Tuple2<Double, Boolean>> graphWithDistances = nextGraphInput.run(new SSSPSemimetric<Long>(50));
	
			// label and remove edges
			newEdges = removeSemimetricEdges(graphWithDistances);
			edgeList = newEdges.collect();
			i++;
		}

		newEdges.map(new FlattenEdgeForOutput()).writeAsCsv(args[1], "\n", "\t");
		env.execute();
	}

	private static DataSet<Edge<Long, Tuple2<Double, Boolean>>> removeSemimetricEdges(
			Graph<Long, Double, Tuple2<Double, Boolean>> graphWithDistances) {

		@SuppressWarnings("serial")
		DataSet<Edge<Long, Tuple2<Double, Boolean>>> newEdges = graphWithDistances.groupReduceOnNeighbors(
				new NeighborsFunctionWithVertexValue<Long, Double, Tuple2<Double,Boolean>,
					Edge<Long, Tuple2<Double, Boolean>>>() {

					public void iterateNeighbors(Vertex<Long, Double> vertex,
							Iterable<Tuple2<Edge<Long, Tuple2<Double, Boolean>>, Vertex<Long, Double>>> neighbors,
							Collector<Edge<Long, Tuple2<Double, Boolean>>> out) {
						if (vertex.f1 == 0) {
							// source vertex
							for (Tuple2<Edge<Long, Tuple2<Double, Boolean>>, Vertex<Long, Double>> neighbor : neighbors) {
								Edge<Long, Tuple2<Double, Boolean>> edge = neighbor.f0;
								Vertex<Long, Double> neighborVertex = neighbor.f1;
								// check if the edge is unlabeled
								if(!edge.getValue().f1) {
									// unlabeled
									if (neighborVertex.getValue() < edge.getValue().f0) {
										// semi-metric ==> remove
									}
									else {
										// metric ==> label and output
										out.collect(new Edge<Long, Tuple2<Double, Boolean>>(edge.getSource(),
												edge.getTarget(),
												new Tuple2<Double, Boolean>(edge.getValue().f0, true)));
									}
								}
								else {
									out.collect(edge);
								}
							}
						}
						else {
							// check if the edge target is the SSSP source
							for (Tuple2<Edge<Long, Tuple2<Double, Boolean>>, Vertex<Long, Double>> neighbor : neighbors) {
								Edge<Long, Tuple2<Double, Boolean>> edge = neighbor.f0;
								Vertex<Long, Double> neighborVertex = neighbor.f1;
								if (neighborVertex.getValue() == 0) {
									// the neighbor is the SSSP source
									// check if the edge is unlabeled
									if(!edge.getValue().f1) {
										// unlabeled
										if (vertex.getValue() < edge.getValue().f0) {
											// semi-metric ==> remove
										}
										else {
											// metric ==> label and output
											out.collect(new Edge<Long, Tuple2<Double, Boolean>>(edge.getSource(),
													edge.getTarget(),
													new Tuple2<Double, Boolean>(edge.getValue().f0, true)));
										}
									}
								}
								else {
									// output the edge
									out.collect(edge);
								}
							}
						}
					}
		}, EdgeDirection.OUT);

		return newEdges;
	}

	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Double>> findNewSource(Graph<Long, Double, Tuple2<Double, Boolean>> graph) {

		// count unlabeled edges per vertex
		DataSet<Tuple3<Long, Double, Integer>> verticesWithUlabeled =
				graph.groupReduceOnEdges(new EdgesFunction<Long, Tuple2<Double, Boolean>,
				Tuple3<Long, Double, Integer>>() {

			public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Tuple2<Double, Boolean>>>> edges,
					Collector<Tuple3<Long, Double, Integer>> out) {

				int unlabeledEdges = 0;
				long vertexId = -1;
				double maxWeight = 0.0;

				for (Tuple2<Long, Edge<Long, Tuple2<Double, Boolean>>> edge : edges) {
					vertexId = edge.f0;
					if (!edge.f1.getValue().f1) {
						// unlabeled
						unlabeledEdges++;
						if (edge.f1.getValue().f0 > maxWeight) {
							maxWeight = edge.f1.getValue().f0;
						}
					}
				}
				out.collect(new Tuple3<Long, Double, Integer>(vertexId, maxWeight, unlabeledEdges));
			}
		}, EdgeDirection.OUT);

		// find the vertex with the most unlabeled edges
		DataSet<Tuple2<Long, Double>> sourceWithMaxWeight = verticesWithUlabeled.maxBy(2).project(0, 1);
		
		// initialize vertex values
		DataSet<Vertex<Long, Double>> initialVertices = graph.getVertices().map(
				new InitializeSourceValue()).withBroadcastSet(sourceWithMaxWeight, "sourceAndWeight");

		return initialVertices;
	}

	@SuppressWarnings("serial")
	private static final class InitializeSourceValue extends 
		RichMapFunction<Vertex<Long, Double>, Vertex<Long, Double>> {

		long sourceId;

		@SuppressWarnings("unchecked")
		@Override
		public void open(Configuration parameters) throws Exception {
			sourceId = ((Tuple2<Long, Double>) getRuntimeContext()
					.getBroadcastVariable("sourceAndWeight").get(0)).f0;
		}

		public Vertex<Long, Double> map(Vertex<Long, Double> vertex) {
			if (vertex.f0.equals(sourceId)) {
				return new Vertex<Long, Double>(vertex.f0, 0.0);
			}
			else {
				return new Vertex<Long, Double>(vertex.f0, Double.POSITIVE_INFINITY);	
			}
		}
		
	}

	@SuppressWarnings("serial")
	private static final class FlattenEdgeForOutput implements MapFunction<Edge<Long, Tuple2<Double, Boolean>>,
		Tuple4<Long, Long, Double, String>> {

		public Tuple4<Long, Long, Double, String> map(
				Edge<Long, Tuple2<Double, Boolean>> edge) {
			return new Tuple4<Long, Long, Double, String>(edge.f0, edge.f1, edge.f2.f0,
					edge.f2.f1.toString());
		}
	}

	@Override
	public String getDescription() {
		return "Find and remove semi-metric edges";
	}
}