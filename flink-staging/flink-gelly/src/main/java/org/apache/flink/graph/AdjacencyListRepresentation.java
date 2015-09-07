package org.apache.flink.graph;

import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GSAConfiguration;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.graph.validation.GraphValidator;

public class AdjacencyListRepresentation<K, VV, EV> extends Graph<K, VV, EV> {

	@Override
	public ExecutionEnvironment getContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean validate(GraphValidator<K, VV, EV> validator)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Vertex<K, VV>> getVertices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Edge<K, EV>> getEdges() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Tuple2<K, VV>> getVerticesAsTuple2() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Tuple3<K, K, EV>> getEdgesAsTuple3() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Triplet<K, VV, EV>> getTriplets() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <NV> Graph<K, NV, EV> mapVertices(
			MapFunction<Vertex<K, VV>, NV> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <NV> Graph<K, NV, EV> mapVertices(
			MapFunction<Vertex<K, VV>, NV> mapper,
			TypeInformation<Vertex<K, NV>> returnType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <NV> Graph<K, VV, NV> mapEdges(MapFunction<Edge<K, EV>, NV> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <NV> Graph<K, VV, NV> mapEdges(MapFunction<Edge<K, EV>, NV> mapper,
			TypeInformation<Edge<K, NV>> returnType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Graph<K, VV, EV> joinWithVertices(
			DataSet<Tuple2<K, T>> inputDataSet,
			MapFunction<Tuple2<VV, T>, VV> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Graph<K, VV, EV> joinWithEdges(
			DataSet<Tuple3<K, K, T>> inputDataSet,
			MapFunction<Tuple2<EV, T>, EV> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Graph<K, VV, EV> joinWithEdgesOnSource(
			DataSet<Tuple2<K, T>> inputDataSet,
			MapFunction<Tuple2<EV, T>, EV> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Graph<K, VV, EV> joinWithEdgesOnTarget(
			DataSet<Tuple2<K, T>> inputDataSet,
			MapFunction<Tuple2<EV, T>, EV> mapper) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> subgraph(
			FilterFunction<Vertex<K, VV>> vertexFilter,
			FilterFunction<Edge<K, EV>> edgeFilter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> filterOnVertices(
			FilterFunction<Vertex<K, VV>> vertexFilter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> filterOnEdges(FilterFunction<Edge<K, EV>> edgeFilter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Tuple2<K, Long>> outDegrees() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Tuple2<K, Long>> inDegrees() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Tuple2<K, Long>> getDegrees() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> getUndirected() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> DataSet<T> groupReduceOnEdges(
			EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
			EdgeDirection direction) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> DataSet<T> groupReduceOnEdges(
			EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo)
			throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> DataSet<T> groupReduceOnEdges(
			EdgesFunction<K, EV, T> edgesFunction, EdgeDirection direction)
			throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> DataSet<T> groupReduceOnEdges(
			EdgesFunction<K, EV, T> edgesFunction, EdgeDirection direction,
			TypeInformation<T> typeInfo) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> reverse() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long numberOfVertices() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long numberOfEdges() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DataSet<K> getVertexIds() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Tuple2<K, K>> getEdgeIds() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> addVertex(Vertex<K, VV> vertex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> addVertices(List<Vertex<K, VV>> verticesToAdd) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> addEdge(Vertex<K, VV> source, Vertex<K, VV> target,
			EV edgeValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> addEdges(List<Edge<K, EV>> newEdges) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> removeVertex(Vertex<K, VV> vertex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> removeVertices(
			List<Vertex<K, VV>> verticesToBeRemoved) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> removeEdge(Edge<K, EV> edge) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> removeEdges(List<Edge<K, EV>> edgesToBeRemoved) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> union(Graph<K, VV, EV> graph) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<K, VV, EV> difference(Graph<K, VV, EV> graph) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <M> Graph<K, VV, EV> runVertexCentricIteration(
			VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
			MessagingFunction<K, VV, M, EV> messagingFunction,
			int maximumNumberOfIterations) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <M> Graph<K, VV, EV> runVertexCentricIteration(
			VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
			MessagingFunction<K, VV, M, EV> messagingFunction,
			int maximumNumberOfIterations, VertexCentricConfiguration parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <M> Graph<K, VV, EV> runGatherSumApplyIteration(
			GatherFunction<VV, EV, M> gatherFunction,
			SumFunction<VV, EV, M> sumFunction,
			ApplyFunction<K, VV, M> applyFunction, int maximumNumberOfIterations) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <M> Graph<K, VV, EV> runGatherSumApplyIteration(
			GatherFunction<VV, EV, M> gatherFunction,
			SumFunction<VV, EV, M> sumFunction,
			ApplyFunction<K, VV, M> applyFunction,
			int maximumNumberOfIterations, GSAConfiguration parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T run(GraphAlgorithm<K, VV, EV, T> algorithm) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> DataSet<T> groupReduceOnNeighbors(
			NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> DataSet<T> groupReduceOnNeighbors(
			NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo)
			throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> DataSet<T> groupReduceOnNeighbors(
			NeighborsFunction<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> DataSet<T> groupReduceOnNeighbors(
			NeighborsFunction<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo)
			throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Tuple2<K, VV>> reduceOnNeighbors(
			ReduceNeighborsFunction<VV> reduceNeighborsFunction,
			EdgeDirection direction) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataSet<Tuple2<K, EV>> reduceOnEdges(
			ReduceEdgesFunction<EV> reduceEdgesFunction, EdgeDirection direction)
			throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

}
