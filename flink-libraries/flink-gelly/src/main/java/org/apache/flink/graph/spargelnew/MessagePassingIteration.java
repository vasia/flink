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

package org.apache.flink.graph.spargelnew;

import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import com.google.common.base.Preconditions;

/**
 * This class represents iterative graph computations, programmed in a vertex-centric perspective.
 * It is a special case of <i>Bulk Synchronous Parallel</i> computation. The paradigm has also been
 * implemented by Google's <i>Pregel</i> system and by <i>Apache Giraph</i>.
 * <p>
 * Vertex centric algorithms operate on graphs, which are defined through vertices and edges. The 
 * algorithms send messages along the edges and update the state of vertices based on
 * the old state and the incoming messages. All vertices have an initial state.
 * The computation terminates once no vertex receives any message anymore.
 * Additionally, a maximum number of iterations (supersteps) may be specified.
 * <p>
 * The computation is here represented by one function:
 * <ul>
 *   <li>The {@link ComputeFunction} receives incoming messages, may update the state for
 *   the vertex, and sends messages along the edges of the vertex.
 *   </li>
 * </ul>
 * <p>
 *
 * Vertex-centric graph iterations are are run by calling
 * {@link Graph#runVertexCentricIteration(VertexUpdateFunction, MessagingFunction, int)}.
 *
 * @param <K> The type of the vertex key (the vertex identifier).
 * @param <VV> The type of the vertex value (the state of the vertex).
 * @param <Message> The type of the message sent between vertices along the edges.
 * @param <EV> The type of the values that are associated with the edges.
 */
public class MessagePassingIteration<K, VV, EV, Message> 
	implements CustomUnaryOperation<Vertex<K, VV>, Vertex<K, VV>>
{

	private final ComputeFunction<K, VV, EV, Message> computeFunction;
	
	private final DataSet<Edge<K, EV>> edgesWithValue;
	
	private final int maximumNumberOfIterations;
	
	private final TypeInformation<Message> messageType;
	
	private DataSet<Vertex<K, VV>> initialVertices;

	private VertexCentricConfiguration configuration;

	private static final boolean MESSAGE = true;

	private static final boolean VERTEXVALUE = false;

	// ----------------------------------------------------------------------------------
	
	private MessagePassingIteration(ComputeFunction<K, VV, EV, Message> cf,
			DataSet<Edge<K, EV>> edgesWithValue, int maximumNumberOfIterations)
	{
		Preconditions.checkNotNull(cf);
		Preconditions.checkNotNull(edgesWithValue);
		Preconditions.checkArgument(maximumNumberOfIterations > 0,
				"The maximum number of iterations must be at least one.");

		this.computeFunction = cf;
		this.edgesWithValue = edgesWithValue;
		this.maximumNumberOfIterations = maximumNumberOfIterations;		
		this.messageType = getMessageType(cf);
	}
	
	private TypeInformation<Message> getMessageType(ComputeFunction<K, VV, EV, Message> cf) {
		return TypeExtractor.createTypeInfo(ComputeFunction.class, cf.getClass(), 2, null, null);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Custom Operator behavior
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the input data set for this operator. In the case of this operator this input data set represents
	 * the set of vertices with their initial state.
	 * 
	 * @param inputData The input data set, which in the case of this operator represents the set of
	 *                  vertices with their initial state.
	 * 
	 * @see org.apache.flink.api.java.operators.CustomUnaryOperation#setInput(org.apache.flink.api.java.DataSet)
	 */
	@Override
	public void setInput(DataSet<Vertex<K, VV>> inputData) {
		this.initialVertices = inputData;
	}
	
	/**
	 * Creates the operator that represents this vertex-centric graph computation.
	 * 
	 * @return The operator that represents this vertex-centric graph computation.
	 */
	@Override
	public DataSet<Vertex<K, VV>> createResult() {
		if (this.initialVertices == null) {
			throw new IllegalStateException("The input data set has not been set.");
		}

		// prepare some type information
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) initialVertices.getType()).getTypeAt(0);
		TypeInformation<Tuple2<K, Message>> messageTypeInfo =
				new TupleTypeInfo<Tuple2<K, Message>>(keyType, messageType);
		TypeInformation<Vertex<K, VV>> vertexType = initialVertices.getType();
		TypeInformation<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>> intermediateTypeInfo =
				new TupleTypeInfo<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>>(
						vertexType, messageTypeInfo,
						TypeExtractor.getForClass(Boolean.class));

		DataSet<Tuple2<K, MessageIterator<Message>>> initialWorkSet = initialVertices.map(
				new InitWorkSet<K, VV, Message>());

		final DeltaIteration<Vertex<K, VV>,	Tuple2<K, MessageIterator<Message>>> iteration =
				initialVertices.iterateDelta(initialWorkSet, this.maximumNumberOfIterations, 0);
				setUpIteration(iteration);

		// join with the current state to get vertex values
		DataSet<Tuple2<Vertex<K, VV>, MessageIterator<Message>>> verticesWithMsgs =
				iteration.getSolutionSet().join(iteration.getWorkset())
				.where(0).equalTo(0)
				.with(new AppendVertexState<K, VV, Message>());

		VertexComputeUdf<K, VV, EV, Message> vertexUdf =
				new VertexComputeUdf<K, VV, EV, Message>(computeFunction, intermediateTypeInfo); 

		DataSet<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>> superstepComputation =
				verticesWithMsgs.coGroup(edgesWithValue)
				.where("f0.f0").equalTo(0)
				.with(vertexUdf);

		// compute the solution set delta
		DataSet<Vertex<K, VV>> solutionSetDelta = superstepComputation.flatMap(
				new ProjectNewVertexValue<K, VV, Message>());

		// compute the inbox of each vertex for the next superstep (new workset)
		DataSet<Tuple2<K, MessageIterator<Message>>> newWorkSet = superstepComputation.flatMap(
				new ProjectMessages<K, VV, Message>()).groupBy(0).reduceGroup(
				new CreateMessages<K, Message>());

//		configureComputeFunction(superstepComputation);

		return iteration.closeWith(solutionSetDelta, newWorkSet);
	}

	/**
	 * Creates a new vertex-centric iteration operator for graphs where the edges are associated with a value (such as
	 * a weight or distance).
	 * 
	 * @param edgesWithValue The data set containing edges.
	 * @param uf The function that updates the state of the vertices from the incoming messages.
	 * @param mf The function that turns changed vertex states into messages along the edges.
	 * 
	 * @param <K> The type of the vertex key (the vertex identifier).
	 * @param <VV> The type of the vertex value (the state of the vertex).
	 * @param <Message> The type of the message sent between vertices along the edges.
	 * @param <EV> The type of the values that are associated with the edges.
	 * 
	 * @return An in stance of the vertex-centric graph computation operator.
	 */
	public static final <K, VV, EV, Message> MessagePassingIteration<K, VV, EV, Message> withEdges(
					DataSet<Edge<K, EV>> edgesWithValue,
					ComputeFunction<K, VV, EV, Message> cf,
					int maximumNumberOfIterations)
	{
		return new MessagePassingIteration<K, VV, EV, Message>(cf, edgesWithValue, maximumNumberOfIterations);
	}

	/**
	 * Configures this vertex-centric iteration with the provided parameters.
	 *
	 * @param parameters the configuration parameters
	 */
	public void configure(VertexCentricConfiguration parameters) {
		this.configuration = parameters;
	}

	/**
	 * @return the configuration parameters of this vertex-centric iteration
	 */
	public VertexCentricConfiguration getIterationConfiguration() {
		return this.configuration;
	}

	// --------------------------------------------------------------------------------------------
	//  Wrapping UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static class VertexComputeUdf<K, VV, EV, Message> extends RichCoGroupFunction<
		Tuple2<Vertex<K, VV>, MessageIterator<Message>>, Edge<K, EV>,
		Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>>
		implements ResultTypeQueryable<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>> {

		final ComputeFunction<K, VV, EV, Message> computeFunction;
		private transient TypeInformation<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>> resultType;

		private VertexComputeUdf(ComputeFunction<K, VV, EV, Message> compute,
				TypeInformation<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>> typeInfo) {

			this.computeFunction = compute;
			this.resultType = typeInfo;
		}

		@Override
		public TypeInformation<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>> getProducedType() {
			return this.resultType;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				this.computeFunction.init(getIterationRuntimeContext());
			}
			this.computeFunction.preSuperstep();
		}
		
		@Override
		public void close() throws Exception {
			this.computeFunction.postSuperstep();
		}

		@Override
		public void coGroup(
				Iterable<Tuple2<Vertex<K, VV>, MessageIterator<Message>>> messages,
				Iterable<Edge<K, EV>> edgesIterator,
				Collector<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>> out) throws Exception {

			final Iterator<Tuple2<Vertex<K, VV>, MessageIterator<Message>>> vertexIter =
					messages.iterator();

			if (vertexIter.hasNext()) {

				final Tuple2<Vertex<K, VV>, MessageIterator<Message>> state = vertexIter.next();
				final Vertex<K, VV> vertexState = state.f0;
				final MessageIterator<Message> messageIter = state.f1;

				computeFunction.set(vertexState, edgesIterator.iterator(), out);
				computeFunction.compute(vertexState, messageIter);
			}
		}
	}


	// --------------------------------------------------------------------------------------------
	//  UTIL methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Helper method which sets up an iteration with the given vertex value
	 *
	 * @param iteration
	 */

	private void setUpIteration(DeltaIteration<?, ?> iteration) {

		// set up the iteration operator
		if (this.configuration != null) {

			iteration.name(this.configuration.getName("Vertex-centric iteration (" + computeFunction + ")"));
			iteration.parallelism(this.configuration.getParallelism());
			iteration.setSolutionSetUnManaged(this.configuration.isSolutionSetUnmanagedMemory());

			// register all aggregators
			for (Map.Entry<String, Aggregator<?>> entry : this.configuration.getAggregators().entrySet()) {
				iteration.registerAggregator(entry.getKey(), entry.getValue());
			}
		}
		else {
			// no configuration provided; set default name
			iteration.name("Vertex-centric iteration (" + computeFunction + ")");
		}
	}

	@SuppressWarnings("serial")
	@ForwardedFields("f0")
	private static final class InitWorkSet<K, VV, Message> implements
		MapFunction<Vertex<K, VV>, Tuple2<K, MessageIterator<Message>>> {

		private final MessageIterator<Message> iterator = new MessageIterator<Message>();

		public Tuple2<K, MessageIterator<Message>> map(Vertex<K, VV> vertex) {
			return new Tuple2<K, MessageIterator<Message>>(vertex.getId(), iterator);
		}
	}

	@SuppressWarnings("serial")
	@ForwardedFieldsSecond("f1->f1")
	private static final class AppendVertexState<K, VV, Message> implements
		FlatJoinFunction<Vertex<K, VV>, Tuple2<K, MessageIterator<Message>>,
		Tuple2<Vertex<K, VV>, MessageIterator<Message>>> {

		private Tuple2<Vertex<K, VV>, MessageIterator<Message>> outTuple =
				new Tuple2<Vertex<K, VV>, MessageIterator<Message>>();

		public void join(Vertex<K, VV> vertex, Tuple2<K, MessageIterator<Message>> messages,
				Collector<Tuple2<Vertex<K, VV>, MessageIterator<Message>>> out) {

			outTuple.setField(vertex, 0);
			outTuple.setField(messages.f1, 1);
			out.collect(outTuple);
		}
	}

	@SuppressWarnings("serial")
	private static final class ProjectNewVertexValue<K, VV, Message> implements
		FlatMapFunction<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>, Vertex<K, VV>> {

		public void flatMap(Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean> value,
				Collector<Vertex<K, VV>> out) {

			if (value.f2.equals(VERTEXVALUE)) {
				out.collect(value.f0);
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class ProjectMessages<K, VV, Message> implements
		FlatMapFunction<Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean>, Tuple2<K, Message>> {

		public void flatMap(Tuple3<Vertex<K, VV>, Tuple2<K, Message>, Boolean> value,
				Collector<Tuple2<K, Message>> out) {

			if (value.f2.equals(MESSAGE)) {
				out.collect(value.f1);
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class CreateMessages<K, Message> implements
		GroupReduceFunction<Tuple2<K, Message>, Tuple2<K, MessageIterator<Message>>> {

		final MessageIterator<Message> msgIterator = new MessageIterator<Message>();
		private Tuple2<K, MessageIterator<Message>> outTuple = new Tuple2<K, MessageIterator<Message>>();
		
		public void reduce(Iterable<Tuple2<K, Message>> messages,
				Collector<Tuple2<K, MessageIterator<Message>>> out) {

			final Iterator<Tuple2<K, Message>> messagesIter = messages.iterator();

			if (messagesIter.hasNext()) {
				final Tuple2<K, Message> first = messagesIter.next();
				final K id = first.f0;
				msgIterator.setFirst(first.f1);

				@SuppressWarnings("unchecked")
				Iterator<Tuple2<?, Message>> downcastIter =
						(Iterator<Tuple2<?, Message>>) (Iterator<?>) messagesIter;
				msgIterator.setSource(downcastIter);

				outTuple.setField(id, 0);
				outTuple.setField(msgIterator, 1);

				out.collect(outTuple);
			}
		}
	}

}