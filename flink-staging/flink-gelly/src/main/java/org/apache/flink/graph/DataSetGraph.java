package org.apache.flink.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GSAConfiguration;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.GatherSumApplyIteration;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.graph.spargel.VertexCentricIteration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.graph.utils.EdgeToTuple3Map;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.graph.validation.GraphValidator;
import org.apache.flink.util.Collector;

/**
* Represents a Graph consisting of {@link Edge edges} and {@link Vertex
* vertices}
* 
**/
@SuppressWarnings("serial")
public class DataSetGraph<K, VV, EV> extends Graph<K, VV, EV> {

	private final DataSet<Vertex<K, VV>> vertices;
	private final DataSet<Edge<K, EV>> edges;
	private final ExecutionEnvironment context;

	/**
	 * Creates a graph from two DataSets: vertices and edges.
	 * This method chooses the default DataSet representation.
	 * 
	 * @param vertices a DataSet of vertices.
	 * @param edges a DataSet of edges.
	 * @param context the flink execution environment.
	 */
	public DataSetGraph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges,
			ExecutionEnvironment context) {
		this.vertices = vertices;
		this.edges = edges;
		this.context = context;
	}


	@Override
	public DataSet<Vertex<K, VV>> getVertices() {
		return vertices;
	}

	@Override
	public DataSet<Edge<K, EV>> getEdges() {
		return edges;
	}

	@Override
	public DataSet<Tuple2<K, VV>> getVerticesAsTuple2() {
		return vertices.map(new VertexToTuple2Map<K, VV>());
	}

	@Override
	public DataSet<Tuple3<K, K, EV>> getEdgesAsTuple3() {
		return edges.map(new EdgeToTuple3Map<K, EV>());
	}

	@Override
	public DataSet<Triplet<K, VV, EV>> getTriplets() {
		return this.getVertices().join(this.getEdges()).where(0).equalTo(0)
				.with(new ProjectEdgeWithSrcValue<K, VV, EV>())
				.join(this.getVertices()).where(1).equalTo(0)
				.with(new ProjectEdgeWithVertexValues<K, VV, EV>());
	}

	@ForwardedFieldsFirst("f1->f2")
	@ForwardedFieldsSecond("f0; f1; f2->f3")
	private static final class ProjectEdgeWithSrcValue<K, VV, EV> implements
			FlatJoinFunction<Vertex<K, VV>, Edge<K, EV>, Tuple4<K, K, VV, EV>> {

		@Override
		public void join(Vertex<K, VV> vertex, Edge<K, EV> edge, Collector<Tuple4<K, K, VV, EV>> collector)
				throws Exception {

			collector.collect(new Tuple4<K, K, VV, EV>(edge.getSource(), edge.getTarget(), vertex.getValue(),
					edge.getValue()));
		}
	}

	@ForwardedFieldsFirst("f0; f1; f2; f3->f4")
	@ForwardedFieldsSecond("f1->f3")
	private static final class ProjectEdgeWithVertexValues<K, VV, EV> implements
			FlatJoinFunction<Tuple4<K, K, VV, EV>, Vertex<K, VV>, Triplet<K, VV, EV>> {

		@Override
		public void join(Tuple4<K, K, VV, EV> tripletWithSrcValSet,
						Vertex<K, VV> vertex, Collector<Triplet<K, VV, EV>> collector) throws Exception {

			collector.collect(new Triplet<K, VV, EV>(tripletWithSrcValSet.f0, tripletWithSrcValSet.f1,
					tripletWithSrcValSet.f2, vertex.getValue(), tripletWithSrcValSet.f3));
		}
	}

	@Override
	public <NV> Graph<K, NV, EV> mapVertices(MapFunction<Vertex<K, VV>, NV> mapper) {

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);

		TypeInformation<NV> valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, null, null);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		TypeInformation<Vertex<K, NV>> returnType = (TypeInformation<Vertex<K, NV>>) new TupleTypeInfo(
				Vertex.class, keyType, valueType);

		return mapVertices(mapper, returnType);
	}

	@Override
	public <NV> Graph<K, NV, EV> mapVertices(final MapFunction<Vertex<K, VV>, NV> mapper,
			TypeInformation<Vertex<K, NV>> returnType) {

		DataSet<Vertex<K, NV>> mappedVertices = vertices.map(
				new MapFunction<Vertex<K, VV>, Vertex<K, NV>>() {
					public Vertex<K, NV> map(Vertex<K, VV> value) throws Exception {
						return new Vertex<K, NV>(value.f0, mapper.map(value));
					}
				})
				.returns(returnType)
				.withForwardedFields("f0");

		return new DataSetGraph<K, NV, EV>(mappedVertices, edges, this.context);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <NV> Graph<K, VV, NV> mapEdges(MapFunction<Edge<K, EV>, NV> mapper) {
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);

		TypeInformation<NV> valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, null, null);

		@SuppressWarnings("rawtypes")
		TypeInformation<Edge<K, NV>> returnType = (TypeInformation<Edge<K, NV>>) new TupleTypeInfo(
				Edge.class, keyType, keyType, valueType);

		return mapEdges(mapper, returnType);

	}

	@Override
	public <NV> Graph<K, VV, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> mapper,
			TypeInformation<Edge<K, NV>> returnType) {
		DataSet<Edge<K, NV>> mappedEdges = edges.map(
				new MapFunction<Edge<K, EV>, Edge<K, NV>>() {
					public Edge<K, NV> map(Edge<K, EV> value) throws Exception {
						return new Edge<K, NV>(value.f0, value.f1, mapper.map(value));
					}
				})
				.returns(returnType)
				.withForwardedFields("f0; f1");

		return new DataSetGraph<K, VV, NV>(this.vertices, mappedEdges, this.context);
	}

	@Override
	public <T> Graph<K, VV, EV> joinWithVertices(DataSet<Tuple2<K, T>> inputDataSet,
			MapFunction<Tuple2<VV, T>, VV> mapper) {

		DataSet<Vertex<K, VV>> resultedVertices = this.getVertices()
				.coGroup(inputDataSet).where(0).equalTo(0)
				.with(new ApplyCoGroupToVertexValues<K, VV, T>(mapper));
		return new DataSetGraph<K, VV, EV>(resultedVertices, this.edges, this.context);
	}

	private static final class ApplyCoGroupToVertexValues<K, VV, T>
		implements CoGroupFunction<Vertex<K, VV>, Tuple2<K, T>, Vertex<K, VV>> {

		private MapFunction<Tuple2<VV, T>, VV> mapper;

		ApplyCoGroupToVertexValues(MapFunction<Tuple2<VV, T>, VV> mapper) {
			this.mapper = mapper;
		}
		
		@Override
		public void coGroup(Iterable<Vertex<K, VV>> vertices,
				Iterable<Tuple2<K, T>> input, Collector<Vertex<K, VV>> collector) throws Exception {
		
			final Iterator<Vertex<K, VV>> vertexIterator = vertices.iterator();
			final Iterator<Tuple2<K, T>> inputIterator = input.iterator();
		
			if (vertexIterator.hasNext()) {
				if (inputIterator.hasNext()) {
					final Tuple2<K, T> inputNext = inputIterator.next();
		
					collector.collect(new Vertex<K, VV>(inputNext.f0, mapper
							.map(new Tuple2<VV, T>(vertexIterator.next().f1,
									inputNext.f1))));
				} else {
					collector.collect(vertexIterator.next());
				}
		
			}
		}
	}

	@Override
	public <T> Graph<K, VV, EV> joinWithEdges(DataSet<Tuple3<K, K, T>> inputDataSet,
			MapFunction<Tuple2<EV, T>, EV> mapper) {

		DataSet<Edge<K, EV>> resultedEdges = this.getEdges()
				.coGroup(inputDataSet).where(0, 1).equalTo(0, 1)
				.with(new ApplyCoGroupToEdgeValues<K, EV, T>(mapper));
		return new DataSetGraph<K, VV, EV>(this.vertices, resultedEdges, this.context);
	}

	private static final class ApplyCoGroupToEdgeValues<K, EV, T>
		implements CoGroupFunction<Edge<K, EV>, Tuple3<K, K, T>, Edge<K, EV>> {

		private MapFunction<Tuple2<EV, T>, EV> mapper;
		
		ApplyCoGroupToEdgeValues(MapFunction<Tuple2<EV, T>, EV> mapper) {
			this.mapper = mapper;
		}
		
		@Override
		public void coGroup(Iterable<Edge<K, EV>> edges, Iterable<Tuple3<K, K, T>> input,
				Collector<Edge<K, EV>> collector) throws Exception {
		
			final Iterator<Edge<K, EV>> edgesIterator = edges.iterator();
			final Iterator<Tuple3<K, K, T>> inputIterator = input.iterator();
		
			if (edgesIterator.hasNext()) {
				if (inputIterator.hasNext()) {
					final Tuple3<K, K, T> inputNext = inputIterator.next();
		
					collector.collect(new Edge<K, EV>(inputNext.f0,
							inputNext.f1, mapper.map(new Tuple2<EV, T>(
									edgesIterator.next().f2, inputNext.f2))));
				} else {
					collector.collect(edgesIterator.next());
				}
			}
		}
	}

	@Override
	public <T> Graph<K, VV, EV> joinWithEdgesOnSource(DataSet<Tuple2<K, T>> inputDataSet,
			MapFunction<Tuple2<EV, T>, EV> mapper) {
		DataSet<Edge<K, EV>> resultedEdges = this.getEdges()
				.coGroup(inputDataSet).where(0).equalTo(0)
				.with(new ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget<K, EV, T>(mapper));

		return new DataSetGraph<K, VV, EV>(this.vertices, resultedEdges, this.context);
	}

	private static final class ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget<K, EV, T>
		implements CoGroupFunction<Edge<K, EV>, Tuple2<K, T>, Edge<K, EV>> {

		private MapFunction<Tuple2<EV, T>, EV> mapper;
		
		public ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget(
				MapFunction<Tuple2<EV, T>, EV> mapper) {
			this.mapper = mapper;
		}
		
		@Override
		public void coGroup(Iterable<Edge<K, EV>> edges,
				Iterable<Tuple2<K, T>> input, Collector<Edge<K, EV>> collector) throws Exception {
		
			final Iterator<Edge<K, EV>> edgesIterator = edges.iterator();
			final Iterator<Tuple2<K, T>> inputIterator = input.iterator();
		
			if (inputIterator.hasNext()) {
				final Tuple2<K, T> inputNext = inputIterator.next();
		
				while (edgesIterator.hasNext()) {
					Edge<K, EV> edgesNext = edgesIterator.next();
		
					collector.collect(new Edge<K, EV>(edgesNext.f0,
							edgesNext.f1, mapper.map(new Tuple2<EV, T>(
									edgesNext.f2, inputNext.f1))));
				}
		
			} else {
				while (edgesIterator.hasNext()) {
					collector.collect(edgesIterator.next());
				}
			}
		}
	}

	@Override
	public <T> Graph<K, VV, EV> joinWithEdgesOnTarget(DataSet<Tuple2<K, T>> inputDataSet,
			MapFunction<Tuple2<EV, T>, EV> mapper) {
		DataSet<Edge<K, EV>> resultedEdges = this.getEdges()
				.coGroup(inputDataSet).where(1).equalTo(0)
				.with(new ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget<K, EV, T>(mapper));

		return new DataSetGraph<K, VV, EV>(this.vertices, resultedEdges, this.context);
	}

	@Override
	public Graph<K, VV, EV> subgraph(FilterFunction<Vertex<K, VV>> vertexFilter,
			FilterFunction<Edge<K, EV>> edgeFilter) {
		DataSet<Vertex<K, VV>> filteredVertices = this.vertices.filter(vertexFilter);

		DataSet<Edge<K, EV>> remainingEdges = this.edges.join(filteredVertices)
				.where(0).equalTo(0).with(new ProjectEdge<K, VV, EV>())
				.join(filteredVertices).where(1).equalTo(0)
				.with(new ProjectEdge<K, VV, EV>());

		DataSet<Edge<K, EV>> filteredEdges = remainingEdges.filter(edgeFilter);

		return new DataSetGraph<K, VV, EV>(filteredVertices, filteredEdges,
				this.context);
	}

	@ForwardedFieldsFirst("f0; f1; f2")
	private static final class ProjectEdge<K, VV, EV> implements FlatJoinFunction<
		Edge<K, EV>, Vertex<K, VV>, Edge<K, EV>> {
		public void join(Edge<K, EV> first, Vertex<K, VV> second, Collector<Edge<K, EV>> out) {
			out.collect(first);
		}
	}

	@Override
	public Graph<K, VV, EV> filterOnVertices(FilterFunction<Vertex<K, VV>> vertexFilter) {

		DataSet<Vertex<K, VV>> filteredVertices = this.vertices.filter(vertexFilter);
		DataSet<Edge<K, EV>> remainingEdges = this.edges.join(filteredVertices)
				.where(0).equalTo(0).with(new ProjectEdge<K, VV, EV>())
				.join(filteredVertices).where(1).equalTo(0)
				.with(new ProjectEdge<K, VV, EV>());

		return new DataSetGraph<K, VV, EV>(filteredVertices, remainingEdges, this.context);
	}

	@Override
	public Graph<K, VV, EV> filterOnEdges(FilterFunction<Edge<K, EV>> edgeFilter) {

		DataSet<Edge<K, EV>> filteredEdges = this.edges.filter(edgeFilter);
		return new DataSetGraph<K, VV, EV>(this.vertices, filteredEdges, this.context);
	}

	@Override
	public DataSet<Tuple2<K, Long>> outDegrees() {
		return vertices.coGroup(edges).where(0).equalTo(0).with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	private static final class CountNeighborsCoGroup<K, VV, EV>
		implements CoGroupFunction<Vertex<K, VV>, Edge<K, EV>, Tuple2<K, Long>> {

		@SuppressWarnings("unused")
		public void coGroup(Iterable<Vertex<K, VV>> vertex,	Iterable<Edge<K, EV>> outEdges,
				Collector<Tuple2<K, Long>> out) {
			long count = 0;
			for (Edge<K, EV> edge : outEdges) {
				count++;
			}
		
			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();
		
			if(vertexIterator.hasNext()) {
				out.collect(new Tuple2<K, Long>(vertexIterator.next().f0, count));
			} else {
				throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
			}
		}
	}

	@Override
	public DataSet<Tuple2<K, Long>> inDegrees() {
		return vertices.coGroup(edges).where(0).equalTo(1).with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	@Override
	public DataSet<Tuple2<K, Long>> getDegrees() {
		return outDegrees().union(inDegrees()).groupBy(0).sum(1);
	}

	@Override
	public ExecutionEnvironment getContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean validate(GraphValidator<K, VV, EV> validator)
			throws Exception {
		return validator.validate(this);
	}

	@Override
	public Graph<K, VV, EV> getUndirected() {
		DataSet<Edge<K, EV>> undirectedEdges = edges.flatMap(new RegularAndReversedEdgesMap<K, EV>());
		return new DataSetGraph<K, VV, EV>(vertices, undirectedEdges, this.context);
	}

	@Override
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
			EdgeDirection direction) throws IllegalArgumentException {
		//TODO: implement
				return null;
	}

	private static final class ApplyCoGroupFunction<K, VV, EV, T> implements CoGroupFunction<
		Vertex<K, VV>, Edge<K, EV>, T>, ResultTypeQueryable<T> {

		private EdgesFunctionWithVertexValue<K, VV, EV, T> function;
	
		public ApplyCoGroupFunction(EdgesFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}
	
		public void coGroup(Iterable<Vertex<K, VV>> vertex,
				Iterable<Edge<K, EV>> edges, Collector<T> out) throws Exception {
	
			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();
	
			if(vertexIterator.hasNext()) {
				function.iterateEdges(vertexIterator.next(), edges, out);
			} else {
				throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
			}
		}
	
		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFunctionWithVertexValue.class, function.getClass(), 3,
					null, null);
		}
	}

	private static final class ApplyCoGroupFunctionOnAllEdges<K, VV, EV, T>
			implements	CoGroupFunction<Vertex<K, VV>, Tuple2<K, Edge<K, EV>>, T>, ResultTypeQueryable<T> {

		private EdgesFunctionWithVertexValue<K, VV, EV, T> function;
	
		public ApplyCoGroupFunctionOnAllEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}
	
		public void coGroup(Iterable<Vertex<K, VV>> vertex,	final Iterable<Tuple2<K, Edge<K, EV>>> keysWithEdges,
				Collector<T> out) throws Exception {
	
			final Iterator<Edge<K, EV>> edgesIterator = new Iterator<Edge<K, EV>>() {
	
				final Iterator<Tuple2<K, Edge<K, EV>>> keysWithEdgesIterator = keysWithEdges.iterator();
	
				@Override
				public boolean hasNext() {
					return keysWithEdgesIterator.hasNext();
				}
	
				@Override
				public Edge<K, EV> next() {
					return keysWithEdgesIterator.next().f1;
				}
	
				@Override
				public void remove() {
					keysWithEdgesIterator.remove();
				}
			};
	
			Iterable<Edge<K, EV>> edgesIterable = new Iterable<Edge<K, EV>>() {
				public Iterator<Edge<K, EV>> iterator() {
					return edgesIterator;
				}
			};
	
			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();
	
			if(vertexIterator.hasNext()) {
				function.iterateEdges(vertexIterator.next(), edgesIterable, out);
			} else {
				throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
			}
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFunctionWithVertexValue.class, function.getClass(), 3,
					null, null);
		}
	}

	@Override
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo)
			throws IllegalArgumentException {
		switch (direction) {
		case IN:
			return vertices.coGroup(edges).where(0).equalTo(1)
					.with(new ApplyCoGroupFunction<K, VV, EV, T>(edgesFunction));
		case OUT:
			return vertices.coGroup(edges).where(0).equalTo(0)
					.with(new ApplyCoGroupFunction<K, VV, EV, T>(edgesFunction));
		case ALL:
			return vertices.coGroup(edges.flatMap(new EmitOneEdgePerNode<K, VV, EV>()))
					.where(0).equalTo(0).with(new ApplyCoGroupFunctionOnAllEdges<K, VV, EV, T>(edgesFunction));
		default:
			throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	@Override
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunction<K, EV, T> edgesFunction, EdgeDirection direction)
			throws IllegalArgumentException {
		//TODO: implement
				return null;
	}

	@Override
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunction<K, EV, T> edgesFunction, EdgeDirection direction,
			TypeInformation<T> typeInfo) throws IllegalArgumentException {

			switch (direction) {
			case IN:
				return edges.map(new ProjectVertexIdMap<K, EV>(1))
						.withForwardedFields("f1->f0")
						.groupBy(0).reduceGroup(new ApplyGroupReduceFunction<K, EV, T>(edgesFunction)).returns(typeInfo);
			case OUT:
				return edges.map(new ProjectVertexIdMap<K, EV>(0))
						.withForwardedFields("f0")
						.groupBy(0).reduceGroup(new ApplyGroupReduceFunction<K, EV, T>(edgesFunction)).returns(typeInfo);
			case ALL:
				return edges.flatMap(new EmitOneEdgePerNode<K, VV, EV>())
						.groupBy(0).reduceGroup(new ApplyGroupReduceFunction<K, EV, T>(edgesFunction)).returns(typeInfo);
			default:
				throw new IllegalArgumentException("Illegal edge direction");
			}
	}

	@Override
	public Graph<K, VV, EV> reverse() {
		DataSet<Edge<K, EV>> reversedEdges = edges.map(new ReverseEdgesMap<K, EV>());
		return new DataSetGraph<K, VV, EV>(vertices, reversedEdges, this.context);
	}

	@Override
	public long numberOfVertices() throws Exception {
		return vertices.count();
	}

	@Override
	public long numberOfEdges() throws Exception {
		return edges.count();
	}

	@Override
	public DataSet<K> getVertexIds() {
		return vertices.map(new ExtractVertexIDMapper<K, VV>());
	}

	private static final class ExtractVertexIDMapper<K, VV>
		implements MapFunction<Vertex<K, VV>, K> {

		@Override
		public K map(Vertex<K, VV> vertex) {
			return vertex.f0;
		}
	}
	
	@Override
	public DataSet<Tuple2<K, K>> getEdgeIds() {
		return edges.map(new ExtractEdgeIDsMapper<K, EV>());
	}

	@ForwardedFields("f0; f1")
	private static final class ExtractEdgeIDsMapper<K, EV>
			implements MapFunction<Edge<K, EV>, Tuple2<K, K>> {
		@Override
		public Tuple2<K, K> map(Edge<K, EV> edge) throws Exception {
			return new Tuple2<K, K>(edge.f0, edge.f1);
		}
	}

	@Override
	public Graph<K, VV, EV> addVertex(final Vertex<K, VV> vertex) {
		List<Vertex<K, VV>> newVertex = new ArrayList<Vertex<K, VV>>();
		newVertex.add(vertex);
		return addVertices(newVertex);
	}

	@Override
	public Graph<K, VV, EV> addVertices(List<Vertex<K, VV>> verticesToAdd) {
		DataSet<Vertex<K, VV>> newVertices = this.vertices.union(this.context.fromCollection(verticesToAdd)).distinct();
		return new DataSetGraph<K, VV, EV>(newVertices, this.edges, this.context);
	}

	@Override
	public Graph<K, VV, EV> addEdge(Vertex<K, VV> source, Vertex<K, VV> target,
			EV edgeValue) {
				Graph<K, VV, EV> partialGraph = fromCollection(Arrays.asList(source, target),
						Arrays.asList(new Edge<K, EV>(source.f0, target.f0, edgeValue)),
						this.context);
				return this.union(partialGraph);
	}

	@Override
	public Graph<K, VV, EV> addEdges(List<Edge<K, EV>> newEdges) {

		DataSet<Edge<K,EV>> newEdgesDataSet = this.context.fromCollection(newEdges);

		DataSet<Edge<K,EV>> validNewEdges = this.getVertices().join(newEdgesDataSet)
				.where(0).equalTo(0)
				.with(new JoinVerticesWithEdgesOnSrc<K, VV, EV>())
				.join(this.getVertices()).where(1).equalTo(0)
				.with(new JoinWithVerticesOnTrg<K, VV, EV>());

		return fromDataSet(this.vertices, this.edges.union(validNewEdges), this.context);
	}

	@Override
	public Graph<K, VV, EV> removeVertex(Vertex<K, VV> vertex) {

		List<Vertex<K, VV>> vertexToBeRemoved = new ArrayList<Vertex<K, VV>>();
		vertexToBeRemoved.add(vertex);

		return removeVertices(vertexToBeRemoved);
	}

	@Override
	public Graph<K, VV, EV> removeVertices(List<Vertex<K, VV>> verticesToBeRemoved) {
		return removeVertices(this.context.fromCollection(verticesToBeRemoved));
	}

	private Graph<K, VV, EV> removeVertices(DataSet<Vertex<K, VV>> verticesToBeRemoved) {

		DataSet<Vertex<K, VV>> newVertices = getVertices().coGroup(verticesToBeRemoved).where(0).equalTo(0)
				.with(new VerticesRemovalCoGroup<K, VV>());

		DataSet < Edge < K, EV >> newEdges = newVertices.join(getEdges()).where(0).equalTo(0)
				// if the edge source was removed, the edge will also be removed
				.with(new ProjectEdgeToBeRemoved<K, VV, EV>())
				// if the edge target was removed, the edge will also be removed
				.join(newVertices).where(1).equalTo(0)
				.with(new ProjectEdge<K, VV, EV>());

		return new DataSetGraph<K, VV, EV>(newVertices, newEdges, context);
	}

	private static final class VerticesRemovalCoGroup<K, VV> implements CoGroupFunction<Vertex<K, VV>, Vertex<K, VV>, Vertex<K, VV>> {

		@Override
		public void coGroup(Iterable<Vertex<K, VV>> vertex, Iterable<Vertex<K, VV>> vertexToBeRemoved,
							Collector<Vertex<K, VV>> out) throws Exception {

			final Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();
			final Iterator<Vertex<K, VV>> vertexToBeRemovedIterator = vertexToBeRemoved.iterator();
			Vertex<K, VV> next;

			if (vertexIterator.hasNext()) {
				if (!vertexToBeRemovedIterator.hasNext()) {
					next = vertexIterator.next();
					out.collect(next);
				}
			}
		}
	}



	@ForwardedFieldsSecond("f0; f1; f2")
	private static final class ProjectEdgeToBeRemoved<K,VV,EV> implements JoinFunction<Vertex<K, VV>, Edge<K, EV>, Edge<K, EV>> {
		@Override
		public Edge<K, EV> join(Vertex<K, VV> vertex, Edge<K, EV> edge) throws Exception {
			return edge;
		}
	}

	@Override
	public Graph<K, VV, EV> removeEdge(Edge<K, EV> edge) {
		DataSet<Edge<K, EV>> newEdges = getEdges().filter(new EdgeRemovalEdgeFilter<K, EV>(edge));
		return new DataSetGraph<K, VV, EV>(this.vertices, newEdges, this.context);
	}

	private static final class EdgeRemovalEdgeFilter<K, EV> implements FilterFunction<Edge<K, EV>> {
		private Edge<K, EV> edgeToRemove;

		public EdgeRemovalEdgeFilter(Edge<K, EV> edge) {
			edgeToRemove = edge;
		}

		@Override
		public boolean filter(Edge<K, EV> edge) {
			return (!(edge.f0.equals(edgeToRemove.f0) && edge.f1
					.equals(edgeToRemove.f1)));
		}
	}

	@Override
	public Graph<K, VV, EV> removeEdges(List<Edge<K, EV>> edgesToBeRemoved) {

		DataSet<Edge<K, EV>> newEdges = getEdges().coGroup(this.context.fromCollection(edgesToBeRemoved))
				.where(0,1).equalTo(0,1).with(new EdgeRemovalCoGroup<K, EV>());

		return new DataSetGraph<K, VV, EV>(this.vertices, newEdges, context);
	}

	private static final class EdgeRemovalCoGroup<K,EV> implements CoGroupFunction<Edge<K, EV>, Edge<K, EV>, Edge<K, EV>> {

		@Override
		public void coGroup(Iterable<Edge<K, EV>> edge, Iterable<Edge<K, EV>> edgeToBeRemoved,
							Collector<Edge<K, EV>> out) throws Exception {

			final Iterator<Edge<K, EV>> edgeIterator = edge.iterator();
			final Iterator<Edge<K, EV>> edgeToBeRemovedIterator = edgeToBeRemoved.iterator();
			Edge<K, EV> next;

			if (edgeIterator.hasNext()) {
				if (!edgeToBeRemovedIterator.hasNext()) {
					next = edgeIterator.next();
					out.collect(next);
				}
			}
		}
	}

	@Override
	public Graph<K, VV, EV> union(Graph<K, VV, EV> graph) {
		DataSet<Vertex<K, VV>> unionedVertices = graph.getVertices().union(this.getVertices()).distinct();
		DataSet<Edge<K, EV>> unionedEdges = graph.getEdges().union(this.getEdges());
		return new DataSetGraph<K, VV, EV>(unionedVertices, unionedEdges, this.context);
	}

	@Override
	public Graph<K, VV, EV> difference(Graph<K, VV, EV> graph) {
		DataSet<Vertex<K,VV>> removeVerticesData = graph.getVertices();
		return this.removeVertices(removeVerticesData);
	}

	@Override
	public <M> Graph<K, VV, EV> runVertexCentricIteration(
			VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
			MessagingFunction<K, VV, M, EV> messagingFunction,
			int maximumNumberOfIterations) {

				return this.runVertexCentricIteration(vertexUpdateFunction, messagingFunction,
						maximumNumberOfIterations, null);
			}


	@Override
	public <M> Graph<K, VV, EV> runVertexCentricIteration(
			VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
			MessagingFunction<K, VV, M, EV> messagingFunction,
			int maximumNumberOfIterations, VertexCentricConfiguration parameters) {

				VertexCentricIteration<K, VV, M, EV> iteration = VertexCentricIteration.withEdges(
						edges, vertexUpdateFunction, messagingFunction, maximumNumberOfIterations);

				iteration.configure(parameters);

				DataSet<Vertex<K, VV>> newVertices = this.getVertices().runOperation(iteration);

				return new DataSetGraph<K, VV, EV>(newVertices, this.edges, this.context);
	}

	@Override
	public <M> Graph<K, VV, EV> runGatherSumApplyIteration(
			GatherFunction<VV, EV, M> gatherFunction,
			SumFunction<VV, EV, M> sumFunction,
			ApplyFunction<K, VV, M> applyFunction, int maximumNumberOfIterations) {

				return this.runGatherSumApplyIteration(gatherFunction, sumFunction, applyFunction,
						maximumNumberOfIterations, null);
	}

	@Override
	public <M> Graph<K, VV, EV> runGatherSumApplyIteration(
			GatherFunction<VV, EV, M> gatherFunction,
			SumFunction<VV, EV, M> sumFunction,
			ApplyFunction<K, VV, M> applyFunction,
			int maximumNumberOfIterations, GSAConfiguration parameters) {

				GatherSumApplyIteration<K, VV, EV, M> iteration = GatherSumApplyIteration.withEdges(
						edges, gatherFunction, sumFunction, applyFunction, maximumNumberOfIterations);

				iteration.configure(parameters);

				DataSet<Vertex<K, VV>> newVertices = vertices.runOperation(iteration);

				return new DataSetGraph<K, VV, EV>(newVertices, this.edges, this.context);
	}

	@Override
	public <T> T run(GraphAlgorithm<K, VV, EV, T> algorithm) throws Exception {
		return algorithm.run(this);
	}

	@Override
	public <T> DataSet<T> groupReduceOnNeighbors(
			NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction) throws IllegalArgumentException {
				switch (direction) {
				case IN:
					// create <edge-sourceVertex> pairs
					DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
							.join(this.vertices).where(0).equalTo(0);
					return vertices.coGroup(edgesWithSources)
							.where(0).equalTo("f0.f1")
							.with(new ApplyNeighborCoGroupFunction<K, VV, EV, T>(neighborsFunction));
				case OUT:
					// create <edge-targetVertex> pairs
					DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
							.join(this.vertices).where(1).equalTo(0);
					return vertices.coGroup(edgesWithTargets)
							.where(0).equalTo("f0.f0")
							.with(new ApplyNeighborCoGroupFunction<K, VV, EV, T>(neighborsFunction));
				case ALL:
					// create <edge-sourceOrTargetVertex> pairs
					DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
							.flatMap(new EmitOneEdgeWithNeighborPerNode<K, EV>())
							.join(this.vertices).where(1).equalTo(0)
							.with(new ProjectEdgeWithNeighbor<K, VV, EV>());

					return vertices.coGroup(edgesWithNeighbors)
							.where(0).equalTo(0)
							.with(new ApplyCoGroupFunctionOnAllNeighbors<K, VV, EV, T>(neighborsFunction));
				default:
					throw new IllegalArgumentException("Illegal edge direction");
				}
			}

	@Override
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo)
			throws IllegalArgumentException {
				switch (direction) {
				case IN:
					// create <edge-sourceVertex> pairs
					DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
							.join(this.vertices).where(0).equalTo(0);
					return vertices.coGroup(edgesWithSources)
							.where(0).equalTo("f0.f1")
							.with(new ApplyNeighborCoGroupFunction<K, VV, EV, T>(neighborsFunction)).returns(typeInfo);
				case OUT:
					// create <edge-targetVertex> pairs
					DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
							.join(this.vertices).where(1).equalTo(0);
					return vertices.coGroup(edgesWithTargets)
							.where(0).equalTo("f0.f0")
							.with(new ApplyNeighborCoGroupFunction<K, VV, EV, T>(neighborsFunction)).returns(typeInfo);
				case ALL:
					// create <edge-sourceOrTargetVertex> pairs
					DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
							.flatMap(new EmitOneEdgeWithNeighborPerNode<K, EV>())
							.join(this.vertices).where(1).equalTo(0)
							.with(new ProjectEdgeWithNeighbor<K, VV, EV>());

					return vertices.coGroup(edgesWithNeighbors)
							.where(0).equalTo(0)
							.with(new ApplyCoGroupFunctionOnAllNeighbors<K, VV, EV, T>(neighborsFunction)).returns(typeInfo);
				default:
					throw new IllegalArgumentException("Illegal edge direction");
			}
		}

	@Override
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunction<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction) throws IllegalArgumentException {
				switch (direction) {
				case IN:
					// create <edge-sourceVertex> pairs
					DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
							.join(this.vertices).where(0).equalTo(0)
							.with(new ProjectVertexIdJoin<K, VV, EV>(1))
							.withForwardedFieldsFirst("f1->f0");
					return edgesWithSources.groupBy(0).reduceGroup(
							new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction));
				case OUT:
					// create <edge-targetVertex> pairs
					DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
							.join(this.vertices).where(1).equalTo(0)
							.with(new ProjectVertexIdJoin<K, VV, EV>(0))
							.withForwardedFieldsFirst("f0");
					return edgesWithTargets.groupBy(0).reduceGroup(
							new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction));
				case ALL:
					// create <edge-sourceOrTargetVertex> pairs
					DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
							.flatMap(new EmitOneEdgeWithNeighborPerNode<K, EV>())
							.join(this.vertices).where(1).equalTo(0)
							.with(new ProjectEdgeWithNeighbor<K, VV, EV>());

					return edgesWithNeighbors.groupBy(0).reduceGroup(
							new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction));
				default:
					throw new IllegalArgumentException("Illegal edge direction");
				}
			}


	@Override
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunction<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo)
			throws IllegalArgumentException {
				switch (direction) {
				case IN:
					// create <edge-sourceVertex> pairs
					DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
							.join(this.vertices).where(0).equalTo(0)
							.with(new ProjectVertexIdJoin<K, VV, EV>(1))
							.withForwardedFieldsFirst("f1->f0");
					return edgesWithSources.groupBy(0).reduceGroup(
							new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction)).returns(typeInfo);
				case OUT:
					// create <edge-targetVertex> pairs
					DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
							.join(this.vertices).where(1).equalTo(0)
							.with(new ProjectVertexIdJoin<K, VV, EV>(0))
							.withForwardedFieldsFirst("f0");
					return edgesWithTargets.groupBy(0).reduceGroup(
							new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction)).returns(typeInfo);
				case ALL:
					// create <edge-sourceOrTargetVertex> pairs
					DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
							.flatMap(new EmitOneEdgeWithNeighborPerNode<K, EV>())
							.join(this.vertices).where(1).equalTo(0)
							.with(new ProjectEdgeWithNeighbor<K, VV, EV>());

					return edgesWithNeighbors.groupBy(0).reduceGroup(
							new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction)).returns(typeInfo);
				default:
					throw new IllegalArgumentException("Illegal edge direction");
			}
		}

	private static final class ApplyNeighborGroupReduceFunction<K, VV, EV, T>
		implements GroupReduceFunction<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>, T>, ResultTypeQueryable<T> {

		private NeighborsFunction<K, VV, EV, T> function;
		
		public ApplyNeighborGroupReduceFunction(NeighborsFunction<K, VV, EV, T> fun) {
			this.function = fun;
		}
		
		public void reduce(Iterable<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edges, Collector<T> out) throws Exception {
			function.iterateNeighbors(edges, out);
		}
		
		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunction.class, function.getClass(), 3, null, null);
		}
	}

	@ForwardedFieldsSecond("f1")
	private static final class ProjectVertexWithNeighborValueJoin<K, VV, EV>
		implements FlatJoinFunction<Edge<K, EV>, Vertex<K, VV>, Tuple2<K, VV>> {
	
		private int fieldPosition;
		
		public ProjectVertexWithNeighborValueJoin(int position) {
			this.fieldPosition = position;
		}
		
		@SuppressWarnings("unchecked")
		public void join(Edge<K, EV> edge, Vertex<K, VV> otherVertex, 
				Collector<Tuple2<K, VV>> out) {
			out.collect(new Tuple2<K, VV>((K) edge.getField(fieldPosition), otherVertex.getValue()));
		}
	}

	private static final class ProjectVertexIdJoin<K, VV, EV> implements FlatJoinFunction<
		Edge<K, EV>, Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> {
	
		private int fieldPosition;
		
		public ProjectVertexIdJoin(int position) {
			this.fieldPosition = position;
		}
		
		@SuppressWarnings("unchecked")
		public void join(Edge<K, EV> edge, Vertex<K, VV> otherVertex,
						Collector<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> out) {
			out.collect(new Tuple3<K, Edge<K, EV>, Vertex<K, VV>>((K) edge.getField(fieldPosition), edge, otherVertex));
		}
	}

	@ForwardedFieldsFirst("f0")
	@ForwardedFieldsSecond("f1")
	private static final class ProjectNeighborValue<K, VV, EV> implements FlatJoinFunction<
		Tuple3<K, K, Edge<K, EV>>, Vertex<K, VV>, Tuple2<K, VV>> {
	
		public void join(Tuple3<K, K, Edge<K, EV>> keysWithEdge, Vertex<K, VV> neighbor,
				Collector<Tuple2<K, VV>> out) {
		
			out.collect(new Tuple2<K, VV>(keysWithEdge.f0, neighbor.getValue()));
		}
	}

	@ForwardedFieldsFirst("f0; f2->f1")
	@ForwardedFieldsSecond("*->f2")
	private static final class ProjectEdgeWithNeighbor<K, VV, EV> implements FlatJoinFunction<
		Tuple3<K, K, Edge<K, EV>>, Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> {
	
		public void join(Tuple3<K, K, Edge<K, EV>> keysWithEdge, Vertex<K, VV> neighbor,
						Collector<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> out) {
			out.collect(new Tuple3<K, Edge<K, EV>, Vertex<K, VV>>(keysWithEdge.f0, keysWithEdge.f2, neighbor));
		}
	}
		
	private static final class ApplyNeighborCoGroupFunction<K, VV, EV, T> implements CoGroupFunction<
		Vertex<K, VV>, Tuple2<Edge<K, EV>, Vertex<K, VV>>, T>, ResultTypeQueryable<T> {
	
		private NeighborsFunctionWithVertexValue<K, VV, EV, T> function;
		
		public ApplyNeighborCoGroupFunction(NeighborsFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}
		
		public void coGroup(Iterable<Vertex<K, VV>> vertex, Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighbors,
				Collector<T> out) throws Exception {
			function.iterateNeighbors(vertex.iterator().next(),	neighbors, out);
		}
		
		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunctionWithVertexValue.class,	function.getClass(), 3, null, null);
		}
	}

	@Override
	public DataSet<Tuple2<K, VV>> reduceOnNeighbors(ReduceNeighborsFunction<VV> reduceNeighborsFunction,
			EdgeDirection direction) throws IllegalArgumentException {
				switch (direction) {
				case IN:
					// create <vertex-source value> pairs
					final DataSet<Tuple2<K, VV>> verticesWithSourceNeighborValues = edges
							.join(this.vertices).where(0).equalTo(0)
							.with(new ProjectVertexWithNeighborValueJoin<K, VV, EV>(1))
							.withForwardedFieldsFirst("f1->f0");
					return verticesWithSourceNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<K, VV>(
							reduceNeighborsFunction));
				case OUT:
					// create <vertex-target value> pairs
					DataSet<Tuple2<K, VV>> verticesWithTargetNeighborValues = edges
							.join(this.vertices).where(1).equalTo(0)
							.with(new ProjectVertexWithNeighborValueJoin<K, VV, EV>(0))
							.withForwardedFieldsFirst("f0");
					return verticesWithTargetNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<K, VV>(
							reduceNeighborsFunction));
				case ALL:
					// create <vertex-neighbor value> pairs
					DataSet<Tuple2<K, VV>> verticesWithNeighborValues = edges
							.flatMap(new EmitOneEdgeWithNeighborPerNode<K, EV>())
							.join(this.vertices).where(1).equalTo(0)
							.with(new ProjectNeighborValue<K, VV, EV>());

					return verticesWithNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<K, VV>(
							reduceNeighborsFunction));
				default:
					throw new IllegalArgumentException("Illegal edge direction");
			}
		}

	@ForwardedFields("f0")
	private static final class ApplyNeighborReduceFunction<K, VV> implements ReduceFunction<Tuple2<K, VV>> {

		private ReduceNeighborsFunction<VV> function;

		public ApplyNeighborReduceFunction(ReduceNeighborsFunction<VV> fun) {
			this.function = fun;
		}

		@Override
		public Tuple2<K, VV> reduce(Tuple2<K, VV> first, Tuple2<K, VV> second) throws Exception {
			first.setField(function.reduceNeighbors(first.f1, second.f1), 1);
			return first;
		}
	}

	@Override
	public DataSet<Tuple2<K, EV>> reduceOnEdges(ReduceEdgesFunction<EV> reduceEdgesFunction, EdgeDirection direction)
		throws IllegalArgumentException {

			switch (direction) {
				case IN:
					return edges.map(new ProjectVertexWithEdgeValueMap<K, EV>(1))
							.withForwardedFields("f1->f0")
							.groupBy(0).reduce(new ApplyReduceFunction<K, EV>(reduceEdgesFunction));
				case OUT:
					return edges.map(new ProjectVertexWithEdgeValueMap<K, EV>(0))
							.withForwardedFields("f0->f0")
							.groupBy(0).reduce(new ApplyReduceFunction<K, EV>(reduceEdgesFunction));
				case ALL:
					return edges.flatMap(new EmitOneVertexWithEdgeValuePerNode<K, EV>())
							.withForwardedFields("f2->f1")
							.groupBy(0).reduce(new ApplyReduceFunction<K, EV>(reduceEdgesFunction));
				default:
					throw new IllegalArgumentException("Illegal edge direction");
			}
		}

	@ForwardedFields("f0")
	private static final class ApplyReduceFunction<K, EV> implements ReduceFunction<Tuple2<K, EV>> {

		private ReduceEdgesFunction<EV> function;

		public ApplyReduceFunction(ReduceEdgesFunction<EV> fun) {
			this.function = fun;
		}

		@Override
		public Tuple2<K, EV> reduce(Tuple2<K, EV> first, Tuple2<K, EV> second) throws Exception {
			first.setField(function.reduceEdges(first.f1, second.f1), 1);
			return first;
		}
	}

	private static final class ApplyCoGroupFunctionOnAllNeighbors<K, VV, EV, T>
		implements CoGroupFunction<Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>, T>, ResultTypeQueryable<T> {

		private NeighborsFunctionWithVertexValue<K, VV, EV, T> function;
		
		public ApplyCoGroupFunctionOnAllNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}
	
		public void coGroup(Iterable<Vertex<K, VV>> vertex,
				final Iterable<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> keysWithNeighbors, 
				Collector<T> out) throws Exception {
		
			final Iterator<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighborsIterator = new Iterator<Tuple2<Edge<K, EV>, Vertex<K, VV>>>() {
		
				final Iterator<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> keysWithEdgesIterator = keysWithNeighbors.iterator();
		
				@Override
				public boolean hasNext() {
					return keysWithEdgesIterator.hasNext();
				}
		
				@Override
				public Tuple2<Edge<K, EV>, Vertex<K, VV>> next() {
					Tuple3<K, Edge<K, EV>, Vertex<K, VV>> next = keysWithEdgesIterator.next();
					return new Tuple2<Edge<K, EV>, Vertex<K, VV>>(next.f1, next.f2);
				}
		
				@Override
				public void remove() {
					keysWithEdgesIterator.remove();
				}
			};
		
			Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighborsIterable = new Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>>() {
				public Iterator<Tuple2<Edge<K, EV>, Vertex<K, VV>>> iterator() {
					return neighborsIterator;
				}
			};
		
			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();
		
			if (vertexIterator.hasNext()) {
				function.iterateNeighbors(vertexIterator.next(), neighborsIterable, out);
			} else {
				throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
			}
		}
		
		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunctionWithVertexValue.class,	function.getClass(), 3, null, null);
		}
	}

	private static final class ProjectVertexIdMap<K, EV> implements MapFunction<
		Edge<K, EV>, Tuple2<K, Edge<K, EV>>> {
	
		private int fieldPosition;
	
		public ProjectVertexIdMap(int position) {
			this.fieldPosition = position;
		}
	
		@SuppressWarnings("unchecked")
		public Tuple2<K, Edge<K, EV>> map(Edge<K, EV> edge) {
			return new Tuple2<K, Edge<K, EV>>((K) edge.getField(fieldPosition),	edge);
		}
	}

	private static final class ProjectVertexWithEdgeValueMap<K, EV>	implements MapFunction<
		Edge<K, EV>, Tuple2<K, EV>> {
	
		private int fieldPosition;
	
		public ProjectVertexWithEdgeValueMap(int position) {
			this.fieldPosition = position;
		}
	
		@SuppressWarnings("unchecked")
		public Tuple2<K, EV> map(Edge<K, EV> edge) {
			return new Tuple2<K, EV>((K) edge.getField(fieldPosition),	edge.getValue());
		}
	}
	
	private static final class ApplyGroupReduceFunction<K, EV, T> implements GroupReduceFunction<
		Tuple2<K, Edge<K, EV>>, T>,	ResultTypeQueryable<T> {
	
		private EdgesFunction<K, EV, T> function;
	
		public ApplyGroupReduceFunction(EdgesFunction<K, EV, T> fun) {
			this.function = fun;
		}
	
		public void reduce(Iterable<Tuple2<K, Edge<K, EV>>> edges, Collector<T> out) throws Exception {
			function.iterateEdges(edges, out);
		}
	
		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFunction.class, function.getClass(), 2, null, null);
		}
	}
	
	private static final class EmitOneEdgePerNode<K, VV, EV> implements FlatMapFunction<
		Edge<K, EV>, Tuple2<K, Edge<K, EV>>> {
	
		public void flatMap(Edge<K, EV> edge, Collector<Tuple2<K, Edge<K, EV>>> out) {
			out.collect(new Tuple2<K, Edge<K, EV>>(edge.getSource(), edge));
			out.collect(new Tuple2<K, Edge<K, EV>>(edge.getTarget(), edge));
		}
	}
	
	private static final class EmitOneVertexWithEdgeValuePerNode<K, EV>	implements FlatMapFunction<
		Edge<K, EV>, Tuple2<K, EV>> {
	
		public void flatMap(Edge<K, EV> edge, Collector<Tuple2<K, EV>> out) {
			out.collect(new Tuple2<K, EV>(edge.getSource(), edge.getValue()));
			out.collect(new Tuple2<K, EV>(edge.getTarget(), edge.getValue()));
		}
	}
	
	private static final class EmitOneEdgeWithNeighborPerNode<K, EV> implements FlatMapFunction<
		Edge<K, EV>, Tuple3<K, K, Edge<K, EV>>> {
	
		public void flatMap(Edge<K, EV> edge, Collector<Tuple3<K, K, Edge<K, EV>>> out) {
			out.collect(new Tuple3<K, K, Edge<K, EV>>(edge.getSource(), edge.getTarget(), edge));
			out.collect(new Tuple3<K, K, Edge<K, EV>>(edge.getTarget(), edge.getSource(), edge));
		}
	}
	
	@ForwardedFields("f0->f1; f1->f0; f2")
	private static final class ReverseEdgesMap<K, EV>
			implements MapFunction<Edge<K, EV>, Edge<K, EV>> {
	
		public Edge<K, EV> map(Edge<K, EV> value) {
			return new Edge<K, EV>(value.f1, value.f0, value.f2);
		}
	}
	
	private static final class RegularAndReversedEdgesMap<K, EV>
			implements FlatMapFunction<Edge<K, EV>, Edge<K, EV>> {
	
		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Edge<K, EV>> out) throws Exception {
			out.collect(new Edge<K, EV>(edge.f0, edge.f1, edge.f2));
			out.collect(new Edge<K, EV>(edge.f1, edge.f0, edge.f2));
		}
	}

	@ForwardedFieldsSecond("f0; f1; f2")
	private static final class JoinVerticesWithEdgesOnSrc<K, VV, EV> implements
			JoinFunction<Vertex<K, VV>, Edge<K, EV>, Edge<K, EV>> {

		@Override
		public Edge<K, EV> join(Vertex<K, VV> vertex, Edge<K, EV> edge) throws Exception {
			return edge;
		}
	}

	@ForwardedFieldsFirst("f0; f1; f2")
	private static final class JoinWithVerticesOnTrg<K, VV, EV> implements
			JoinFunction<Edge<K, EV>, Vertex<K, VV>, Edge<K, EV>> {

		@Override
		public Edge<K, EV> join(Edge<K, EV> edge, Vertex<K, VV> vertex) throws Exception {
			return edge;
		}
	}

}
