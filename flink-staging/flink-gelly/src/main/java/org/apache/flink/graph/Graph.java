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

package org.apache.flink.graph;

import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GSAConfiguration;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.graph.validation.GraphValidator;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * @see org.apache.flink.graph.Edge
 * @see org.apache.flink.graph.Vertex
 * 
 * @param <K> the key type for edge and vertex identifiers
 * @param <VV> the value type for vertices
 * @param <EV> the value type for edges
 */
public abstract class Graph<K, VV, EV> {

	/**
	 * Creates a graph from a DataSet of vertices and a DataSet of edges.
	 * 
	 * @param vertices a DataSet of vertices.
	 * @param edges a DataSet of edges.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromDataSet(DataSet<Vertex<K, VV>> vertices,
			DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {

		return new DataSetGraph<K, VV, EV>(vertices, edges, context);
	}

	/**
	 * Creates a graph from a Collection of vertices and a Collection of edges.
	 * 
	 * @param vertices a Collection of vertices.
	 * @param edges a Collection of edges.
	 * @param context the Flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromCollection(Collection<Vertex<K, VV>> vertices,
			Collection<Edge<K, EV>> edges, ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(vertices),
				context.fromCollection(edges), context);
	}

	/**
	 * Creates a graph from a Collection of edges, vertices are induced from the
	 * edges. Vertices are created automatically and their values are set to
	 * NullValue.
	 * 
	 * @param edges a Collection of vertices.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, EV> Graph<K, NullValue, EV> fromCollection(Collection<Edge<K, EV>> edges,
			ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(edges), context);
	}

	/**
	 * Creates a graph from a Collection of edges, vertices are induced from the
	 * edges and vertex values are calculated by a mapper function. Vertices are
	 * created automatically and their values are set by applying the provided
	 * map function to the vertex ids.
	 * 
	 * @param edges a Collection of edges.
	 * @param mapper the mapper function.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromCollection(Collection<Edge<K, EV>> edges,
			final MapFunction<K, VV> mapper,ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(edges), mapper, context);
	}

	/**
	 * Creates a graph from a DataSet of edges, vertices are induced from the
	 * edges. Vertices are created automatically and their values are set to
	 * NullValue.
	 * 
	 * @param edges a DataSet of edges.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, EV> Graph<K, NullValue, EV> fromDataSet(
			DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {

		DataSet<Vertex<K, NullValue>> vertices = edges.flatMap(new EmitSrcAndTarget<K, EV>()).distinct();

		return new DataSetGraph<K, NullValue, EV>(vertices, edges, context);
	}

	@SuppressWarnings("serial")
	private static final class EmitSrcAndTarget<K, EV> implements FlatMapFunction<
		Edge<K, EV>, Vertex<K, NullValue>> {

		public void flatMap(Edge<K, EV> edge, Collector<Vertex<K, NullValue>> out) {
			out.collect(new Vertex<K, NullValue>(edge.f0, NullValue.getInstance()));
			out.collect(new Vertex<K, NullValue>(edge.f1, NullValue.getInstance()));
		}
	}

	/**
	 * Creates a graph from a DataSet of edges, vertices are induced from the
	 * edges and vertex values are calculated by a mapper function. Vertices are
	 * created automatically and their values are set by applying the provided
	 * map function to the vertex ids.
	 * 
	 * @param edges a DataSet of edges.
	 * @param mapper the mapper function.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	@SuppressWarnings("serial")
	public static <K, VV, EV> Graph<K, VV, EV> fromDataSet(DataSet<Edge<K, EV>> edges,
			final MapFunction<K, VV> mapper, ExecutionEnvironment context) {

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);

		TypeInformation<VV> valueType = TypeExtractor.createTypeInfo(
				MapFunction.class, mapper.getClass(), 1, null, null);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		TypeInformation<Vertex<K, VV>> returnType = (TypeInformation<Vertex<K, VV>>) new TupleTypeInfo(
				Vertex.class, keyType, valueType);

		DataSet<Vertex<K, VV>> vertices = edges
				.flatMap(new EmitSrcAndTargetAsTuple1<K, EV>()).distinct()
				.map(new MapFunction<Tuple1<K>, Vertex<K, VV>>() {
					public Vertex<K, VV> map(Tuple1<K> value) throws Exception {
						return new Vertex<K, VV>(value.f0, mapper.map(value.f0));
					}
				}).returns(returnType).withForwardedFields("f0");

		return new DataSetGraph<K, VV, EV>(vertices, edges, context);
	}

	@SuppressWarnings("serial")
	private static final class EmitSrcAndTargetAsTuple1<K, EV> implements FlatMapFunction<
		Edge<K, EV>, Tuple1<K>> {

		public void flatMap(Edge<K, EV> edge, Collector<Tuple1<K>> out) {
			out.collect(new Tuple1<K>(edge.f0));
			out.collect(new Tuple1<K>(edge.f1));
		}
	}

	/**
	 * Creates a graph from a DataSet of Tuple objects for vertices and edges.
	 * 
	 * Vertices with value are created from Tuple2, Edges with value are created
	 * from Tuple3.
	 * 
	 * @param vertices a DataSet of Tuple2.
	 * @param edges a DataSet of Tuple3.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromTupleDataSet(DataSet<Tuple2<K, VV>> vertices,
			DataSet<Tuple3<K, K, EV>> edges, ExecutionEnvironment context) {

		DataSet<Vertex<K, VV>> vertexDataSet = vertices.map(new Tuple2ToVertexMap<K, VV>());
		DataSet<Edge<K, EV>> edgeDataSet = edges.map(new Tuple3ToEdgeMap<K, EV>());
		return fromDataSet(vertexDataSet, edgeDataSet, context);
	}

	/**
	 * Creates a graph from a DataSet of Tuple objects for edges, vertices are
	 * induced from the edges.
	 * 
	 * Edges with value are created from Tuple3. Vertices are created
	 * automatically and their values are set to NullValue.
	 * 
	 * @param edges a DataSet of Tuple3.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, EV> Graph<K, NullValue, EV> fromTupleDataSet(DataSet<Tuple3<K, K, EV>> edges,
			ExecutionEnvironment context) {

		DataSet<Edge<K, EV>> edgeDataSet = edges.map(new Tuple3ToEdgeMap<K, EV>());
		return fromDataSet(edgeDataSet, context);
	}

	/**
	 * Creates a graph from a DataSet of Tuple objects for edges, vertices are
	 * induced from the edges and vertex values are calculated by a mapper
	 * function. Edges with value are created from Tuple3. Vertices are created
	 * automatically and their values are set by applying the provided map
	 * function to the vertex ids.
	 * 
	 * @param edges a DataSet of Tuple3.
	 * @param mapper the mapper function.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromTupleDataSet(DataSet<Tuple3<K, K, EV>> edges,
			final MapFunction<K, VV> mapper, ExecutionEnvironment context) {

		DataSet<Edge<K, EV>> edgeDataSet = edges.map(new Tuple3ToEdgeMap<K, EV>());
		return fromDataSet(edgeDataSet, mapper, context);
	}

	/**
	 * @return the execution environment.
	 */
	public abstract ExecutionEnvironment getContext();

	/**
	 * Function that checks whether a Graph is a valid Graph,
	 * as defined by the given {@link GraphValidator}.
	 * 
	 * @return true if the Graph is valid.
	 */
	public abstract Boolean validate(GraphValidator<K, VV, EV> validator) throws Exception;

	/**
	 * @return the vertex DataSet.
	 */
	public abstract DataSet<Vertex<K, VV>> getVertices();

	/**
	 * @return the edge DataSet.
	 */
	public abstract DataSet<Edge<K, EV>> getEdges();

	/**
	 * @return the vertex DataSet as Tuple2.
	 */
	public abstract DataSet<Tuple2<K, VV>> getVerticesAsTuple2();

	/**
	 * @return the edge DataSet as Tuple3.
	 */
	public abstract DataSet<Tuple3<K, K, EV>> getEdgesAsTuple3();

	/**
	 * This method allows access to the graph's edge values along with its source and target vertex values.
	 *
	 * @return a triplet DataSet consisting of (srcVertexId, trgVertexId, srcVertexValue, trgVertexValue, edgeValue)
	 */
	public abstract DataSet<Triplet<K, VV, EV>> getTriplets();

	/**
	 * Apply a function to the attribute of each vertex in the graph.
	 * 
	 * @param mapper the map function to apply.
	 * @return a new graph
	 */
	public abstract <NV> Graph<K, NV, EV> mapVertices(final MapFunction<Vertex<K, VV>, NV> mapper);

	/**
	 * Apply a function to the attribute of each vertex in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @param returnType the explicit return type.
	 * @return a new graph
	 */
	abstract <NV> Graph<K, NV, EV> mapVertices(final MapFunction<Vertex<K, VV>, NV> mapper,
			TypeInformation<Vertex<K,NV>> returnType);

	/**
	 * Apply a function to the attribute of each edge in the graph.
	 * 
	 * @param mapper the map function to apply.
	 * @return a new graph
	 */
	public abstract <NV> Graph<K, VV, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> mapper);

	/**
	 * Apply a function to the attribute of each edge in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @param returnType the explicit return type.
	 * @return a new graph
	 */
	abstract <NV> Graph<K, VV, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> mapper,
			TypeInformation<Edge<K,NV>> returnType);

	/**
	 * Joins the vertex DataSet of this graph with an input DataSet and applies
	 * a UDF on the resulted values.
	 * 
	 * @param inputDataSet the DataSet to join with.
	 * @param mapper the UDF map function to apply.
	 * @return a new graph where the vertex values have been updated.
	 */
	public abstract <T> Graph<K, VV, EV> joinWithVertices(DataSet<Tuple2<K, T>> inputDataSet, 
			final MapFunction<Tuple2<VV, T>, VV> mapper);

	/**
	 * Joins the edge DataSet with an input DataSet on a composite key of both
	 * source and target and applies a UDF on the resulted values.
	 * 
	 * @param inputDataSet the DataSet to join with.
	 * @param mapper the UDF map function to apply.
	 * @param <T> the return type
	 * @return a new graph where the edge values have been updated.
	 */
	public abstract <T> Graph<K, VV, EV> joinWithEdges(DataSet<Tuple3<K, K, T>> inputDataSet,
			final MapFunction<Tuple2<EV, T>, EV> mapper);

	/**
	 * Joins the edge DataSet with an input DataSet on the source key of the
	 * edges and the first attribute of the input DataSet and applies a UDF on
	 * the resulted values. In case the inputDataSet contains the same key more
	 * than once, only the first value will be considered.
	 * 
	 * @param inputDataSet the DataSet to join with.
	 * @param mapper the UDF map function to apply.
	 * @param <T> the return type
	 * @return a new graph where the edge values have been updated.
	 */
	public abstract <T> Graph<K, VV, EV> joinWithEdgesOnSource(DataSet<Tuple2<K, T>> inputDataSet,
			final MapFunction<Tuple2<EV, T>, EV> mapper);

	/**
	 * Joins the edge DataSet with an input DataSet on the target key of the
	 * edges and the first attribute of the input DataSet and applies a UDF on
	 * the resulted values. Should the inputDataSet contain the same key more
	 * than once, only the first value will be considered.
	 * 
	 * @param inputDataSet the DataSet to join with.
	 * @param mapper the UDF map function to apply.
	 * @param <T> the return type
	 * @return a new graph where the edge values have been updated.
	 */
	public abstract <T> Graph<K, VV, EV> joinWithEdgesOnTarget(DataSet<Tuple2<K, T>> inputDataSet,
			final MapFunction<Tuple2<EV, T>, EV> mapper);

	/**
	 * Apply filtering functions to the graph and return a sub-graph that
	 * satisfies the predicates for both vertices and edges.
	 * 
	 * @param vertexFilter the filter function for vertices.
	 * @param edgeFilter the filter function for edges.
	 * @return the resulting sub-graph.
	 */
	public abstract Graph<K, VV, EV> subgraph(FilterFunction<Vertex<K, VV>> vertexFilter,
			FilterFunction<Edge<K, EV>> edgeFilter);

	/**
	 * Apply a filtering function to the graph and return a sub-graph that
	 * satisfies the predicates only for the vertices.
	 * 
	 * @param vertexFilter the filter function for vertices.
	 * @return the resulting sub-graph.
	 */
	public abstract Graph<K, VV, EV> filterOnVertices(FilterFunction<Vertex<K, VV>> vertexFilter);

	/**
	 * Apply a filtering function to the graph and return a sub-graph that
	 * satisfies the predicates only for the edges.
	 * 
	 * @param edgeFilter the filter function for edges.
	 * @return the resulting sub-graph.
	 */
	public abstract Graph<K, VV, EV> filterOnEdges(FilterFunction<Edge<K, EV>> edgeFilter);

	/**
	 * Return the out-degree of all vertices in the graph
	 * 
	 * @return A DataSet of Tuple2<vertexId, outDegree>
	 */
	public abstract DataSet<Tuple2<K, Long>> outDegrees();

	/**
	 * Return the in-degree of all vertices in the graph
	 * 
	 * @return A DataSet of Tuple2<vertexId, inDegree>
	 */
	public abstract DataSet<Tuple2<K, Long>> inDegrees();

	/**
	 * Return the degree of all vertices in the graph
	 * 
	 * @return A DataSet of Tuple2<vertexId, degree>
	 */
	public abstract DataSet<Tuple2<K, Long>> getDegrees();

	/**
	 * This operation adds all inverse-direction edges to the graph.
	 * 
	 * @return the undirected graph.
	 */
	public abstract Graph<K, VV, EV> getUndirected();

	/**
	 * Compute an aggregate over the edges of each vertex. The function applied
	 * on the edges has access to the vertex value.
	 * 
	 * @param edgesFunction
	 *            the function to apply to the neighborhood
	 * @param direction
	 *            the edge direction (in-, out-, all-)
	 * @param <T>
	 *            the output type
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public abstract <T> DataSet<T> groupReduceOnEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
											EdgeDirection direction) throws IllegalArgumentException;

	/**
	 * Compute an aggregate over the edges of each vertex. The function applied
	 * on the edges has access to the vertex value.
	 *
	 * @param edgesFunction
	 *            the function to apply to the neighborhood
	 * @param direction
	 *            the edge direction (in-, out-, all-)
	 * @param <T>
	 *            the output type
	 * @param typeInfo the explicit return type.
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public abstract <T> DataSet<T> groupReduceOnEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
				EdgeDirection direction, TypeInformation<T> typeInfo) throws IllegalArgumentException;

	/**
	 * Compute an aggregate over the edges of each vertex. The function applied
	 * on the edges only has access to the vertex id (not the vertex value).
	 * 
	 * @param edgesFunction
	 *            the function to apply to the neighborhood
	 * @param direction
	 *            the edge direction (in-, out-, all-)
	 * @param <T>
	 *            the output type
	 * @return a dataset of T
	 * @throws IllegalArgumentException
	 */
	public abstract <T> DataSet<T> groupReduceOnEdges(EdgesFunction<K, EV, T> edgesFunction,
											EdgeDirection direction) throws IllegalArgumentException;

	/**
	 * Compute an aggregate over the edges of each vertex. The function applied
	 * on the edges only has access to the vertex id (not the vertex value).
	 *
	 * @param edgesFunction
	 *            the function to apply to the neighborhood
	 * @param direction
	 *            the edge direction (in-, out-, all-)
	 * @param <T>
	 *            the output type
	 * @param typeInfo the explicit return type.
	 * @return a dataset of T
	 * @throws IllegalArgumentException
	 */
	public abstract <T> DataSet<T> groupReduceOnEdges(EdgesFunction<K, EV, T> edgesFunction,
				EdgeDirection direction, TypeInformation<T> typeInfo) throws IllegalArgumentException;

	/**
	 * Reverse the direction of the edges in the graph
	 * 
	 * @return a new graph with all edges reversed
	 */
	public abstract Graph<K, VV, EV> reverse();

	/**
	 * @return a long integer representing the number of vertices
	 */
	public abstract long numberOfVertices() throws Exception;

	/**
	 * @return a long integer representing the number of edges
	 */
	public abstract long numberOfEdges() throws Exception;

	/**
	 * @return The IDs of the vertices as DataSet
	 */
	public abstract DataSet<K> getVertexIds();

	/**
	 * @return The IDs of the edges as DataSet
	 */
	public abstract DataSet<Tuple2<K, K>> getEdgeIds();

	/**
	 * Adds the input vertex to the graph. If the vertex already
	 * exists in the graph, it will not be added again.
	 * 
	 * @param vertex the vertex to be added
	 * @return the new graph containing the existing vertices as well as the one just added
	 */
	public abstract Graph<K, VV, EV> addVertex(final Vertex<K, VV> vertex);

	/**
	 * Adds the list of vertices, passed as input, to the graph.
	 * If the vertices already exist in the graph, they will not be added once more.
	 *
	 * @param verticesToAdd the list of vertices to add
	 * @return the new graph containing the existing and newly added vertices
	 */
	public abstract Graph<K, VV, EV> addVertices(List<Vertex<K, VV>> verticesToAdd);

	/**
	 * Adds the given edge to the graph. If the source and target vertices do
	 * not exist in the graph, they will also be added.
	 * 
	 * @param source the source vertex of the edge
	 * @param target the target vertex of the edge
	 * @param edgeValue the edge value
	 * @return the new graph containing the existing vertices and edges plus the
	 *         newly added edge
	 */
	public abstract Graph<K, VV, EV> addEdge(Vertex<K, VV> source, Vertex<K, VV> target, EV edgeValue);

	/**
	 * Adds the given list edges to the graph.
	 *
	 * When adding an edge for a non-existing set of vertices, the edge is considered invalid and ignored.
	 *
	 * @param newEdges the data set of edges to be added
	 * @return a new graph containing the existing edges plus the newly added edges.
	 */
	public abstract Graph<K, VV, EV> addEdges(List<Edge<K, EV>> newEdges);

	/**
	 * Removes the given vertex and its edges from the graph.
	 * 
	 * @param vertex the vertex to remove
	 * @return the new graph containing the existing vertices and edges without
	 *         the removed vertex and its edges
	 */
	public abstract Graph<K, VV, EV> removeVertex(Vertex<K, VV> vertex);

	/**
	 * Removes the given list of vertices and its edges from the graph.
	 *
	 * @param verticesToBeRemoved the list of vertices to be removed
	 * @return the resulted graph containing the initial vertices and edges minus the vertices
	 * 		   and edges removed.
	 */
	public abstract Graph<K, VV, EV> removeVertices(List<Vertex<K, VV>> verticesToBeRemoved);

	 /**
	 * Removes all edges that match the given edge from the graph.
	 * 
	 * @param edge the edge to remove
	 * @return the new graph containing the existing vertices and edges without
	 *         the removed edges
	 */
	public abstract Graph<K, VV, EV> removeEdge(Edge<K, EV> edge);

	/**
	 * Removes all the edges that match the edges in the given data set from the graph.
	 *
	 * @param edgesToBeRemoved the list of edges to be removed
	 * @return a new graph where the edges have been removed and in which the vertices remained intact
	 */
	public abstract Graph<K, VV, EV> removeEdges(List<Edge<K, EV>> edgesToBeRemoved);

	/**
	 * Performs union on the vertices and edges sets of the input graphs
	 * removing duplicate vertices but maintaining duplicate edges.
	 * 
	 * @param graph the graph to perform union with
	 * @return a new graph
	 */
	public abstract Graph<K, VV, EV> union(Graph<K, VV, EV> graph);

	/**
	 * Performs Difference on the vertex and edge sets of the input graphs
	 * removes common vertices and edges. If a source/target vertex is removed, its corresponding edge will also be removed
	 * @param graph the graph to perform difference with
	 * @return a new graph where the common vertices and edges have been removed
	 */
	public abstract Graph<K,VV,EV> difference(Graph<K,VV,EV> graph);

	/**
	 * Runs a Vertex-Centric iteration on the graph.
	 * No configuration options are provided.
	 *
	 * @param vertexUpdateFunction the vertex update function
	 * @param messagingFunction the messaging function
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * 
	 * @return the updated Graph after the vertex-centric iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public abstract <M> Graph<K, VV, EV> runVertexCentricIteration(
			VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
			MessagingFunction<K, VV, M, EV> messagingFunction,
			int maximumNumberOfIterations);

	/**
	 * Runs a Vertex-Centric iteration on the graph with configuration options.
	 * 
	 * @param vertexUpdateFunction the vertex update function
	 * @param messagingFunction the messaging function
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * @param parameters the iteration configuration parameters
	 * 
	 * @return the updated Graph after the vertex-centric iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public abstract <M> Graph<K, VV, EV> runVertexCentricIteration(
			VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
			MessagingFunction<K, VV, M, EV> messagingFunction,
			int maximumNumberOfIterations, VertexCentricConfiguration parameters);

	/**
	 * Runs a Gather-Sum-Apply iteration on the graph.
	 * No configuration options are provided.
	 *
	 * @param gatherFunction the gather function collects information about adjacent vertices and edges
	 * @param sumFunction the sum function aggregates the gathered information
	 * @param applyFunction the apply function updates the vertex values with the aggregates
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * @param <M> the intermediate type used between gather, sum and apply
	 *
	 * @return the updated Graph after the gather-sum-apply iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public abstract <M> Graph<K, VV, EV> runGatherSumApplyIteration(
			GatherFunction<VV, EV, M> gatherFunction, SumFunction<VV, EV, M> sumFunction,
			ApplyFunction<K, VV, M> applyFunction, int maximumNumberOfIterations);

	/**
	 * Runs a Gather-Sum-Apply iteration on the graph with configuration options.
	 *
	 * @param gatherFunction the gather function collects information about adjacent vertices and edges
	 * @param sumFunction the sum function aggregates the gathered information
	 * @param applyFunction the apply function updates the vertex values with the aggregates
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * @param parameters the iteration configuration parameters
	 * @param <M> the intermediate type used between gather, sum and apply
	 *
	 * @return the updated Graph after the gather-sum-apply iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public abstract <M> Graph<K, VV, EV> runGatherSumApplyIteration(
			GatherFunction<VV, EV, M> gatherFunction, SumFunction<VV, EV, M> sumFunction,
			ApplyFunction<K, VV, M> applyFunction, int maximumNumberOfIterations,
			GSAConfiguration parameters);

	/**
	 * @param algorithm the algorithm to run on the Graph
	 * @param <T> the return type
	 * @return the result of the graph algorithm
	 * @throws Exception
	 */
	public abstract <T> T run(GraphAlgorithm<K, VV, EV, T> algorithm) throws Exception;

	/**
	 * Compute an aggregate over the neighbors (edges and vertices) of each
	 * vertex. The function applied on the neighbors has access to the vertex
	 * value.
	 * 
	 * @param neighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @param <T> the output type
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public abstract <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
												EdgeDirection direction);

	/**
	 * Compute an aggregate over the neighbors (edges and vertices) of each
	 * vertex. The function applied on the neighbors has access to the vertex
	 * value.
	 *
	 * @param neighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @param <T> the output type
	 * @param typeInfo the explicit return type.
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public abstract <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo) throws IllegalArgumentException;


	/**
	 * Compute an aggregate over the neighbors (edges and vertices) of each
	 * vertex. The function applied on the neighbors only has access to the
	 * vertex id (not the vertex value).
	 * 
	 * @param neighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @param <T> the output type
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public abstract <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunction<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction) throws IllegalArgumentException;
	/**
	 * Compute an aggregate over the neighbors (edges and vertices) of each
	 * vertex. The function applied on the neighbors only has access to the
	 * vertex id (not the vertex value).
	 *
	 * @param neighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @param <T> the output type
	 * @param typeInfo the explicit return type.
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public abstract <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunction<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo) throws IllegalArgumentException;

	/**
	 * Compute an aggregate over the neighbor values of each
	 * vertex.
	 *
	 * @param reduceNeighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @return a Dataset containing one value per vertex (vertex id, aggregate vertex value)
	 * @throws IllegalArgumentException
	 */
	public abstract DataSet<Tuple2<K, VV>> reduceOnNeighbors(ReduceNeighborsFunction<VV> reduceNeighborsFunction,
									EdgeDirection direction) throws IllegalArgumentException;

	/**
	 * Compute an aggregate over the edge values of each vertex.
	 *
	 * @param reduceEdgesFunction
	 *            the function to apply to the neighborhood
	 * @param direction
	 *            the edge direction (in-, out-, all-)
	 * @return a Dataset containing one value per vertex(vertex key, aggegate edge value)
	 * @throws IllegalArgumentException
	 */
	public abstract DataSet<Tuple2<K, EV>> reduceOnEdges(ReduceEdgesFunction<EV> reduceEdgesFunction,
								EdgeDirection direction) throws IllegalArgumentException;

}
