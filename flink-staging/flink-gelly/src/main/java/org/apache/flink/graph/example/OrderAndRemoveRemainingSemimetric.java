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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.SSSPSemimetricWithMaxWeight;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.util.Collector;

/**
 * Sort the vertices by the number of adjacent semi-metric edges
 * and run one SSSP per vertex.
 * After a new source has been chosen, check whether it is still a valid source
 * (not all its semi-metric edges have been removed)
 * and retrieve the maximum weight among its semi-metric edges.
 * This weight will be used to stop the SSSP early.
 *
 */
public class OrderAndRemoveRemainingSemimetric implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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
		
		Graph<Long, Double, Tuple2<Double, Boolean>> graph = Graph.fromDataSet(
				newEdges, new MapFunction<Long, Double>() {
						public Double map(Long value) {
							return Double.POSITIVE_INFINITY;
						}
			}, env);

		// get all the vertices that are sources of semi-metric edges and rank them
		DataSet<Tuple2<Long, Long>> sourcesWithRank = rankSources(graph);

		long numSources = sourcesWithRank.count();

		int i = 0;

		while (i < numSources) {
			if (i > 0) {
				graph = Graph.fromCollection(edgeList, new MapFunction<Long, Double>() {
					public Double map(Long value) {
						return Double.POSITIVE_INFINITY;
					}
				}, env);
			}

			Long nextSource = getNextSource(sourcesWithRank, i).collect().get(0).f0;
			
			// initialize the source vertex and set the max weight
			double weight = getMaxWeight(graph, nextSource);
			
			if (weight > -1) {

				DataSet<Vertex<Long, Tuple2<Double, Double>>> initialVertices = initializeVertices(graph, nextSource, weight);
				
				Graph<Long, Tuple2<Double, Double>, Tuple2<Double, Boolean>> nextGraphInput = Graph.fromDataSet(
						initialVertices, graph.getEdges(), env);
		
				// compute sssp
				Graph<Long, Tuple2<Double, Double>, Tuple2<Double, Boolean>> graphWithDistances = nextGraphInput.run(
						new SSSPSemimetricWithMaxWeight<Long>(50));
		
				// label and remove edges
				newEdges = removeSemimetricEdges(graphWithDistances);
				edgeList = newEdges.collect();
			}
			i++;
		}

		newEdges.map(new FlattenEdgeForOutput()).writeAsCsv(args[1], "\n", "\t");
		env.execute();
	}

	@SuppressWarnings("serial")
	private static double getMaxWeight(Graph<Long, Double, Tuple2<Double, Boolean>> graph,	Long nextSource) throws Exception {
		// get the max weight among the semi-metric edges of the source
		DataSet<Edge<Long, Tuple2<Double, Boolean>>> relevantEdges = graph.getEdges()
				.filter(new FilterSourceEdges(nextSource)); 

		double maxWeight = relevantEdges.groupBy(0).reduceGroup(
				new GroupReduceFunction<Edge<Long,Tuple2<Double,Boolean>>, Tuple1<Double>>() {

					public void reduce(Iterable<Edge<Long, Tuple2<Double, Boolean>>> edges,
							Collector<Tuple1<Double>> out) {
						double weight = -1;
						for (Edge<Long, Tuple2<Double, Boolean>> edge : edges) {
							// unlabeled
							if (!edge.f2.f1) {
								if (edge.f2.f0 > weight) {
									weight = edge.f2.f0; 
								}
							}
						}
						out.collect(new Tuple1<Double>(weight));
					}
		}).collect().get(0).f0;
		return maxWeight;
	}

	private static DataSet<Vertex<Long, Tuple2<Double, Double>>> initializeVertices(
			Graph<Long, Double, Tuple2<Double, Boolean>> graph,	Long nextSource, double maxWeight) {
				
		// initialize vertex values
		DataSet<Vertex<Long, Tuple2<Double, Double>>> initialVertices = graph.getVertices().map(
				new InitializeSourceValue(nextSource, maxWeight));

		return initialVertices;
	}

	@SuppressWarnings("serial")
	private static final class FilterSourceEdges extends RichFilterFunction<Edge<Long, Tuple2<Double, Boolean>>> {

		long sourceId;

		public FilterSourceEdges(Long nextSource) {
			this.sourceId = nextSource;
		}

		@Override
		public boolean filter(Edge<Long, Tuple2<Double, Boolean>> edge) {
			return edge.getSource().equals(sourceId);
		}
		
	}

	private static DataSet<Tuple1<Long>> getNextSource(DataSet<Tuple2<Long, Long>> sourcesWithRank, int i) {

		DataSet<Tuple1<Long>> nextSource = sourcesWithRank.flatMap(new SelectSource(i));
		return nextSource;
	}

	@SuppressWarnings("serial")
	@ForwardedFields("f0")
	private static final class SelectSource implements FlatMapFunction<Tuple2<Long, Long>, Tuple1<Long>> {

		private int index;
		public SelectSource(int i) {
			index = i;
		}

		public void flatMap(Tuple2<Long, Long> sourceWithRank, Collector<Tuple1<Long>> out) {
			if (sourceWithRank.f1 == index) {
				out.collect(new Tuple1<Long>(sourceWithRank.f0));
			}
		}
	};

	@SuppressWarnings("serial")
	private static DataSet<Tuple2<Long, Long>> rankSources(Graph<Long, Double, Tuple2<Double, Boolean>> graph) {

		// count unlabeled edges per vertex
		DataSet<Tuple2<Long, Long>> verticesWithUlabeled =
				graph.groupReduceOnEdges(new EdgesFunction<Long, Tuple2<Double, Boolean>,
				Tuple2<Long, Long>>() {

			public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Tuple2<Double, Boolean>>>> edges,
					Collector<Tuple2<Long, Long>> out) {

				long unlabeledEdges = 0;
				long vertexId = -1;

				for (Tuple2<Long, Edge<Long, Tuple2<Double, Boolean>>> edge : edges) {
					vertexId = edge.f0;
					if (!edge.f1.getValue().f1) {
						// unlabeled
						unlabeledEdges++;
					}
				}
				if (unlabeledEdges > 0) {
					out.collect(new Tuple2<Long, Long>(vertexId, unlabeledEdges));
				}
			}
		}, EdgeDirection.OUT);

		// sort
		DataSet<Tuple2<Long, Long>> verticesWithRank = verticesWithUlabeled.map(
				new MapFunction<Tuple2<Long, Long>, Tuple3<Integer, Long, Long>>() {

					public Tuple3<Integer, Long, Long> map(Tuple2<Long, Long> value) {
						return new Tuple3<Integer, Long, Long>(42, value.f0, value.f1);
					}
				}).withForwardedFields("f0->f1; f1->f2")
				.groupBy(0).sortGroup(2, Order.DESCENDING).reduceGroup(
						new GroupReduceFunction<Tuple3<Integer,Long,Long>, Tuple2<Long, Long>>() {

							public void reduce(Iterable<Tuple3<Integer, Long, Long>> values,
									Collector<Tuple2<Long, Long>> out) {
								long i = 0;
								for (Tuple3<Integer, Long, Long> value: values) {
									out.collect(new Tuple2<Long, Long>(value.f1, i));
									i++;
								}
							}
				}).withForwardedFields("f1->f0");
		return verticesWithRank;
	}

	private static DataSet<Edge<Long, Tuple2<Double, Boolean>>> removeSemimetricEdges(
			Graph<Long, Tuple2<Double, Double>, Tuple2<Double, Boolean>> graphWithDistances) {

		@SuppressWarnings("serial")
		DataSet<Edge<Long, Tuple2<Double, Boolean>>> newEdges = graphWithDistances.groupReduceOnNeighbors(
				new NeighborsFunctionWithVertexValue<Long, Tuple2<Double, Double>, Tuple2<Double,Boolean>,
					Edge<Long, Tuple2<Double, Boolean>>>() {

					public void iterateNeighbors(Vertex<Long, Tuple2<Double, Double>> vertex,
							Iterable<Tuple2<Edge<Long, Tuple2<Double, Boolean>>, Vertex<Long, Tuple2<Double, Double>>>> neighbors,
							Collector<Edge<Long, Tuple2<Double, Boolean>>> out) {

						if (vertex.f1.f0 == 0) {
							// source vertex
							for (Tuple2<Edge<Long, Tuple2<Double, Boolean>>, Vertex<Long, Tuple2<Double, Double>>> neighbor : neighbors) {
								Edge<Long, Tuple2<Double, Boolean>> edge = neighbor.f0;
								Vertex<Long, Tuple2<Double, Double>> neighborVertex = neighbor.f1;
								// check if the edge is unlabeled
								if(!edge.getValue().f1) {
									// unlabeled
									if (neighborVertex.f1.f0 < edge.getValue().f0) {
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
							for (Tuple2<Edge<Long, Tuple2<Double, Boolean>>, Vertex<Long, Tuple2<Double, Double>>> neighbor : neighbors) {
								Edge<Long, Tuple2<Double, Boolean>> edge = neighbor.f0;
								Vertex<Long, Tuple2<Double, Double>> neighborVertex = neighbor.f1;
								if (neighborVertex.f1.f0 == 0) {
									// the neighbor is the SSSP source
									// check if the edge is unlabeled
									if(!edge.getValue().f1) {
										// unlabeled
										if (vertex.f1.f0 < edge.getValue().f0) {
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
	@ForwardedFields("f0")
	private static final class InitializeSourceValue extends 
		RichMapFunction<Vertex<Long, Double>, Vertex<Long, Tuple2<Double, Double>>> {

		long sourceId;
		double maxWeight;

		public InitializeSourceValue(Long nextSource, double weight) {
			sourceId = nextSource;
			maxWeight = weight;
		}

		public Vertex<Long, Tuple2<Double, Double>> map(Vertex<Long, Double> vertex) {
			if (vertex.f0.equals(sourceId)) {
				return new Vertex<Long, Tuple2<Double, Double>>(vertex.f0, new Tuple2<Double, Double>(0.0, maxWeight));
			}
			else {
				return new Vertex<Long, Tuple2<Double, Double>>(vertex.f0, 
						new Tuple2<Double, Double>(Double.POSITIVE_INFINITY, maxWeight));	
			}
		}
	}

	@SuppressWarnings("serial")
	@ForwardedFields("f0->f0; f1->f1")
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