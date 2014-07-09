package eu.stratosphere.fixpoint.api;

import java.util.Iterator;

import org.apache.commons.lang3.Validate;

import eu.stratosphere.api.common.aggregators.LongSumAggregator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.operators.CustomUnaryOperation;
import eu.stratosphere.api.java.operators.FlatMapOperator;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.typeutils.ResultTypeQueryable;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.TypeInformation;
import eu.stratosphere.util.Collector;

/**
 * 
 * This class represents a fixed point iteration.
 * It is created by providing a DataSet containing the vertices (parameters) and their initial values,
 * a DataSet containing the edges (dependencies) among vertices with (optionally) their initial weights
 * and a step function.
 * 
 * @param <K> The data type of the vertex keys
 * @param <V> The data type of the vertex values
 * @param <E> The data type of the edge value
 * 
 */
public class FixedPointIteration<K, V, E> implements CustomUnaryOperation<Tuple2<K, V>, Tuple2<K, V>>{

	private DataSet<Tuple2<K, V>> verticesInput;
	private final DataSet<Tuple3<K, K, E>> edgesInputWithValue;
	private final DataSet<Tuple2<K, K>> edgesInputWithoutValue;
	private final int numberOfVertices;
	private final double avgNodeDegree;
	private final StepFunction<K, V, E> stepFunction;
	private final int maxIterations;
	private String name;
	
	private static final String UPDATED_ELEMENTS_AGGR = "updated.elements.aggr";
	
	private FixedPointIteration(DataSet<Tuple3<K, K, E>> edgesWithValue, StepFunction<K, V, E> stepFunction, 
			int maxIterations) {

		Validate.notNull(edgesWithValue);
		Validate.isTrue(maxIterations > 0, "The maximum number of iterations must be at least one.");
		
		// check that the edges are actually a valid tuple set of vertex key types
		TypeInformation<Tuple3<K, K, E>> edgesType = edgesWithValue.getType();
		Validate.isTrue(edgesType.isTupleType() && edgesType.getArity() == 3, "The edges data set (for edges with edge values) must consist of 3-tuples.");
		
		TupleTypeInfo<?> tupleInfo = (TupleTypeInfo<?>) edgesType;
		Validate.isTrue(tupleInfo.getTypeAt(0).equals(tupleInfo.getTypeAt(1)),
			"Both tuple fields (source and target vertex id) must be of the data type that represents the vertex key.");

		this.edgesInputWithValue = edgesWithValue;
		this.edgesInputWithoutValue = null;
		this.maxIterations = maxIterations;
		this.stepFunction  = stepFunction;
		this.numberOfVertices = 9; // verticesInput.count()
		this.avgNodeDegree = 3.0; // edgesInput.count() / numberOfVertices

	}
	
	private FixedPointIteration(DataSet<Tuple2<K, K>> edgesWithoutValue, StepFunction<K, V, E> stepFunction, 
			int maxIterations, boolean noEdgeValue) {
		
		Validate.notNull(edgesWithoutValue);
		Validate.isTrue(maxIterations > 0, "The maximum number of iterations must be at least one.");
		
		// check that the edges are actually a valid tuple set of vertex key types
		TypeInformation<Tuple2<K, K>> edgesType = edgesWithoutValue.getType();
		Validate.isTrue(edgesType.isTupleType() && edgesType.getArity() == 2, "The edges data set (for edges without edge values) "
				+ "must consist of 2-tuples.");
		
		TupleTypeInfo<?> tupleInfo = (TupleTypeInfo<?>) edgesType;
		Validate.isTrue(tupleInfo.getTypeAt(0).equals(tupleInfo.getTypeAt(1)),
			"Both tuple fields (source and target vertex id) must be of the data type that represents the vertex key.");
		
		this.edgesInputWithoutValue = edgesWithoutValue;
		this.edgesInputWithValue = null;
		this.maxIterations = maxIterations;
		this.stepFunction  = stepFunction;
		this.numberOfVertices = 9; // verticesInput.count()
		this.avgNodeDegree = 3.0; // edgesInput.count() / numberOfVertices

	}

	
	// set cost model on/off
	
	// override the default cost model? need access to number of updated elements and avg node degree 
	
	/**
	 * Sets the input data set for this operator. In the case of this operator this input data set represents
	 * the set of vertices (parameters) with their initial state.
	 * 
	 * @param inputData The input data set, which in the case of this operator represents the set of
	 *                  vertices with their initial state.
	 * 
	 * @see eu.stratosphere.api.java.operators.CustomUnaryOperation#setInput(eu.stratosphere.api.java.DataSet)
	 */
	@Override
	public void setInput(DataSet<Tuple2<K, V>> inputData) {
		// check that we really have 2-tuples
		TypeInformation<Tuple2<K, V>> inputType = inputData.getType();
		Validate.isTrue(inputType.isTupleType() && inputType.getArity() == 2, "The input data set (the initial vertices) "
				+ "must consist of 2-tuples.");

		// check that the key type here is the same as for the edges
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) inputType).getTypeAt(0);
		TypeInformation<?> edgeType = edgesInputWithoutValue != null ? edgesInputWithoutValue.getType() : 
			edgesInputWithValue.getType();
		TypeInformation<K> edgeKeyType = ((TupleTypeInfo<?>) edgeType).getTypeAt(0);
		
		Validate.isTrue(keyType.equals(edgeKeyType), "The first tuple field (the vertex id) of the input data set "
				+ "(the initial vertices) must be the same data type as the first fields of the edge data set "
				+ "(the source vertex id). Here, the key type for the vertex ids is '%s' and the key type  for the edges"
				+ " is '%s'.", keyType, edgeKeyType);

		this.verticesInput = inputData;
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public DataSet<Tuple2<K, V>> createResult() {

		if (this.verticesInput == null) {
			throw new IllegalStateException("The input data set has not been set.");
		}
		
		/**
		 * Prepare type information
		 */
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) verticesInput.getType()).getTypeAt(0);
		TypeInformation<Tuple1<K>> tupleKeyType = new TupleTypeInfo<Tuple1<K>>(keyType);
		
		TypeInformation<V> valueType = ((TupleTypeInfo<?>) verticesInput.getType()).getTypeAt(1);
		
		TypeInformation<?>[] vertexTypes = {(BasicTypeInfo<?>)keyType, (BasicTypeInfo<?>)valueType};
		TypeInformation<Tuple2<K, V>> vertexTypeInfo = new TupleTypeInfo<Tuple2<K,V>>(vertexTypes);
		
		TypeInformation<?> edgeType = edgesInputWithoutValue != null ? edgesInputWithoutValue.getType() : 
			edgesInputWithValue.getType();
		
		TypeInformation<?>[] stepFunctionTypesWithoutEdgeValue = {(BasicTypeInfo<?>)keyType, (BasicTypeInfo<?>)keyType, (BasicTypeInfo<?>)valueType}; 
		TypeInformation<Tuple3<K, K, V>> stepFunctionInputTypeWithoutEdgeValue = new TupleTypeInfo<Tuple3<K,K,V>>(stepFunctionTypesWithoutEdgeValue);
		
		TypeInformation<E> edgeValueType = null;
		if (edgesInputWithValue != null) {
			edgeValueType = ((TupleTypeInfo<?>) edgesInputWithValue.getType()).getTypeAt(2);
		}
		
		TypeInformation<?>[] stepFunctionTypesWithEdgeValue = {(BasicTypeInfo<?>)keyType, (BasicTypeInfo<?>)keyType, (BasicTypeInfo<?>)valueType, (BasicTypeInfo<?>)edgeValueType}; 
		TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputTypeWithEdgeValue = new TupleTypeInfo<Tuple4<K,K,V,E>>(stepFunctionTypesWithEdgeValue);
		

		final String name = (this.name != null) ? this.name :
			"Fixpoint iteration (" + stepFunction + ")";
		
		/**
		 * Start with a bulk iteration
		 */
		
		// set up the iteration operator
		IterativeDataSet<Tuple2<K, V>> iteration = verticesInput.iterate(maxIterations);
		iteration.name(name);
		
		// register convergence criterion
		iteration.registerAggregationConvergenceCriterion(UPDATED_ELEMENTS_AGGR, new LongSumAggregator(), 
				new UpdatedElementsCostModelConvergence(numberOfVertices, avgNodeDegree));
		
		DataSet<Tuple2<K, V>> verticesWithNewValues;
		
		if (edgesInputWithValue != null) {
			verticesWithNewValues = getBulkResultWithValuedEdges(iteration, stepFunctionInputTypeWithEdgeValue);
		}
		else {
			verticesWithNewValues = getBulkResultWithPlainEdges(iteration, stepFunctionInputTypeWithoutEdgeValue);
		}
				
		// compare with previous values
		FlatMapOperator<?, Tuple2<K, V>> updatedVertices = verticesWithNewValues.join(iteration)
												.where(0).equalTo(0)
												.flatMap(new AggregateAndEmitUpdatedValue(vertexTypeInfo));
		// close the iteration
		DataSet<Tuple2<K, V>> bulkResult = iteration.closeWith(updatedVertices);


		/**
		 *  Continue with a dependency iteration
		 */

		/**
		 * TODO: find a way to figure out how many iterations out of maxIterations are left. 
		 * If there are no iterations left or no elements changed during the last bulk iteration,
		 * we shouldn't execute any dependency iteration
		 */
		
		DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> depIteration = bulkResult.iterateDelta(bulkResult, maxIterations, 
				0);
		depIteration.name("Dependency iteration:" + name);
		
		DataSet<Tuple2<K, V>> dependencyVerticesWithNewValues;
		
		if (edgesInputWithValue != null) {
			dependencyVerticesWithNewValues = getDepResultWithValuedEdges(depIteration, tupleKeyType, 
					(TypeInformation<Tuple3<K, K, E>>) edgeType, stepFunctionInputTypeWithEdgeValue);
		}
		else {
			dependencyVerticesWithNewValues = getDepResultWithPlainEdges(depIteration, tupleKeyType, 
					(TypeInformation<Tuple2<K, K>>) edgeType, stepFunctionInputTypeWithoutEdgeValue);
		}
		
		// compare with previous values
		FlatMapOperator<?, Tuple2<K, V>> dependencyUpdatedVertices = dependencyVerticesWithNewValues
												.join(depIteration.getSolutionSet())
												.where(0).equalTo(0)
												.flatMap(new EmitOnlyUpdatedValues(vertexTypeInfo));
		// close the iteration
		DataSet<Tuple2<K, V>> result = depIteration.closeWith(dependencyUpdatedVertices, dependencyUpdatedVertices);
		
		return result;
	}

	private DataSet<Tuple2<K, V>> getBulkResultWithPlainEdges(
			IterativeDataSet<Tuple2<K, V>> iteration, TypeInformation<Tuple3<K, K, V>> stepFunctionInputType) {
		// TODO make StepFunction work for edges without values
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> getBulkResultWithValuedEdges(IterativeDataSet<Tuple2<K, V>> iteration, 
			TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputType) {
		
		// produce the DataSet containing each vertex with the in-neighbor and their value		
		FlatMapOperator<?, Tuple4<K, K, V, E>> verticesWithNeighborValues = 
				iteration.join(edgesInputWithValue)
				.where(0).equalTo(0).flatMap(new ProjectStepFunctionInput(stepFunctionInputType));
		
		// result of the step function
		return this.stepFunction.updateState(verticesWithNeighborValues);
	}
	
	private DataSet<Tuple2<K, V>> getDepResultWithPlainEdges(
			DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> depIteration,
			TypeInformation<Tuple1<K>> tupleKeyType, TypeInformation<Tuple2<K, K>> edgeType, 
			TypeInformation<Tuple3<K, K, V>> stepFunctionInputType) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> getDepResultWithValuedEdges(
			DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> depIteration,
			TypeInformation<Tuple1<K>> tupleKeyType, TypeInformation<Tuple3<K, K, E>> edgeType, 
			TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputType) {
		
		FlatMapOperator<?, Tuple1<K>> candidates = depIteration.getWorkset().join(edgesInputWithValue)
																.where(0).equalTo(0).flatMap(new CandidateIDs(tupleKeyType));
		
		DataSet<Tuple1<K>> grouped = candidates.groupBy(0).reduceGroup(new RemoveDuplicatesReduce(tupleKeyType));
		
		DataSet<Tuple3<K, K, E>> candidatesDependencies = grouped.join(edgesInputWithValue).where(0).equalTo(1)
																	.flatMap(new CandidatesDependencies(edgeType));
		
		// produce the DataSet containing each vertex with the in-neighbor and their value		
		FlatMapOperator<?, Tuple4<K, K, V, E>> verticesWithNeighborValues = 
				depIteration.getSolutionSet().join(candidatesDependencies)
				.where(0).equalTo(0).flatMap(new ProjectStepFunctionInput(stepFunctionInputType));
		
		// result of the step function
		return this.stepFunction.updateState(verticesWithNeighborValues);
	}

	/**
	 * Creates a new fixed point iteration operator for dependency graphs where the edges are not associated with a value.
	 * 
	 * @param edgesWithoutValue The data set containing edges. Edges are represented as 2-tuples: (source-id, target-id)
	 * @param stepFunction The step function that updates the state of the vertices from the states of the in-neighbors.
	 * 
	 * @param <K> The type of the vertex key (the vertex identifier).
	 * @param <V> The type of the vertex value (the state of the vertex).
	 * 
	 * @return An in stance of the fixed point computation operator.
	 */
	public static final <K, V> FixedPointIteration<K, V, Object> withPlainEdges(
					DataSet<Tuple2<K, K>> edgesWithoutValue,
						StepFunction<K, V, Object> stepFunction,
						int maximumNumberOfIterations)
	{		
		return new FixedPointIteration<K, V, Object>(edgesWithoutValue, stepFunction, maximumNumberOfIterations, true);
	}
	
	/**
	 * Creates a new fixed point iteration operator for graphs where the edges are associated with a value.
	 * 
	 * @param edgesWithValue The data set containing edges. Edges are represented as 3-tuples: (source-id, target-id, value)
	 * @param stepFunction The step function that updates the state of the vertices from the states of the in-neighbors.
	 * 
	 * @param <K> The type of the vertex key (the vertex identifier).
	 * @param <V> The type of the vertex value (the state of the vertex).
	 * @param <E> The type of the values that are associated with the edges.
	 * 
	 * @return An in stance of the fixed point computation operator.
	 */
	public static final <K, V, E> FixedPointIteration<K, V, E> withValuedEdges(
					DataSet<Tuple3<K, K, E>> edgesWithValue,
					StepFunction<K, V, E> stepFunction,
					int maximumNumberOfIterations)
	{
		return new FixedPointIteration<K, V, E>(edgesWithValue, stepFunction, maximumNumberOfIterations);
	}
	
	/**
	 * Sets the name for the fixpoint iteration. The name is displayed in logs and messages.
	 * 
	 * @param name The name for the iteration.
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * Gets the name from this fixpoint iteration.
	 * 
	 * @return The name of the iteration.
	 */
	public String getName() {
		return name;
	}
	
	private static final class ProjectStepFunctionInput<K, V, E> extends FlatMapFunction
		<Tuple2<Tuple2<K, V>, Tuple3<K, K, E>>, Tuple4<K, K, V, E>> 
		implements ResultTypeQueryable<Tuple4<K, K, V, E>> {
		
		private static final long serialVersionUID = 1L;
		private transient TypeInformation<Tuple4<K, K, V, E>> resultType;
		
		private ProjectStepFunctionInput(TypeInformation<Tuple4<K, K, V, E>> resultType)
		{
			this.resultType = resultType;
		}

		@Override
		public void flatMap(Tuple2<Tuple2<K, V>, Tuple3<K, K, E>> value,
				Collector<Tuple4<K, K, V, E>> out) throws Exception {
			out.collect(new Tuple4<K, K, V, E>
					(value.f1.f1, value.f0.f0, value.f0.f1, value.f1.f2));
			
		}
	
		@Override
		public TypeInformation<Tuple4<K, K, V, E>> getProducedType() {
			return this.resultType;
		}

	}
	
	/** 
	 * for this to work correctly, 
	 * the edges set should contain a self-edge for every vertex
	 *
	 */
	private static final class AggregateAndEmitUpdatedValue<K, V> extends FlatMapFunction
		<Tuple2<Tuple2<K, V>, Tuple2<K, V>>, Tuple2<K, V>> 
		implements ResultTypeQueryable<Tuple2<K, V>> {
		
		private static final long serialVersionUID = 1L;
		private transient TypeInformation<Tuple2<K, V>> resultType;
		private LongSumAggregator updatedElementsAggr;
		
		private AggregateAndEmitUpdatedValue(TypeInformation<Tuple2<K, V>> resultType)
		{
			this.resultType = resultType;
		}
		
		@Override
		public void open(Configuration conf) {
			updatedElementsAggr = getIterationRuntimeContext().getIterationAggregator(UPDATED_ELEMENTS_AGGR);
			int superstep = getIterationRuntimeContext().getSuperstepNumber();
			System.out.println("Bulk Iteration " + superstep);
		}

		@Override
		public void flatMap(Tuple2<Tuple2<K, V>, Tuple2<K, V>> value,
				Collector<Tuple2<K, V>> out) throws Exception {
			
			// count changed elements
			if (!(value.f0.equals(value.f1))) {
				updatedElementsAggr.aggregate(1);
			}
			// emit the updated value
			out.collect(value.f0);
		}
		
		@Override
		public TypeInformation<Tuple2<K, V>> getProducedType() {
			return this.resultType;
		}

	}
	
	private static final class CandidateIDs<K, V, E> extends FlatMapFunction
		<Tuple2<Tuple2<K, V>, Tuple3<K, K, E>>, Tuple1<K>> 
		implements ResultTypeQueryable<Tuple1<K>> {
		
		private static final long serialVersionUID = 1L;
		private transient TypeInformation<Tuple1<K>> resultType;
		
		private CandidateIDs(TypeInformation<Tuple1<K>> resultType)
		{
			this.resultType = resultType;
		}
	
		@Override
		public void flatMap(Tuple2<Tuple2<K, V>, Tuple3<K, K, E>> value,
				Collector<Tuple1<K>> out) throws Exception {
			
			out.collect(new Tuple1<K>(value.f1.f1));
			
		}
	
		@Override
		public TypeInformation<Tuple1<K>> getProducedType() {
			return this.resultType;
		}

	}

	private static final class RemoveDuplicatesReduce<K> extends GroupReduceFunction<Tuple1<K>, Tuple1<K>> 
		implements ResultTypeQueryable<Tuple1<K>> {
		
		private static final long serialVersionUID = 1L;
		private transient TypeInformation<Tuple1<K>> resultType;
		
		private RemoveDuplicatesReduce(TypeInformation<Tuple1<K>> resultType)
		{
			this.resultType = resultType;
		}
	
		@Override
		public void reduce(Iterator<Tuple1<K>> values, Collector<Tuple1<K>> out)
				throws Exception {
			out.collect(values.next());
		}
		
		@Override
		public TypeInformation<Tuple1<K>> getProducedType() {
			return this.resultType;
		}
	
	}
	
	private static final class CandidatesDependencies<K, E> extends FlatMapFunction
		<Tuple2<Tuple1<K>, Tuple3<K, K, E>>, Tuple3<K, K, E>> 
		implements ResultTypeQueryable<Tuple3<K, K, E>> {
	
		private static final long serialVersionUID = 1L;
		private transient TypeInformation<Tuple3<K, K, E>> resultType;
		
		private CandidatesDependencies(TypeInformation<Tuple3<K, K, E>> resultType)
		{
			this.resultType = resultType;
		}
	
		@Override
		public void flatMap(Tuple2<Tuple1<K>, Tuple3<K, K, E>> value,
				Collector<Tuple3<K, K, E>> out) throws Exception {
			
			out.collect(value.f1);
		}
		
		@Override
		public TypeInformation<Tuple3<K, K, E>> getProducedType() {
			return this.resultType;
		}
	
	}

	private static final class EmitOnlyUpdatedValues<K, V> extends FlatMapFunction
		<Tuple2<Tuple2<K, V>, Tuple2<K, V>>, Tuple2<K, V>> 
		implements ResultTypeQueryable<Tuple2<K, V>> {
	
		private static final long serialVersionUID = 1L;
		private transient TypeInformation<Tuple2<K, V>> resultType;
		
		private EmitOnlyUpdatedValues(TypeInformation<Tuple2<K, V>> resultType)
		{
			this.resultType = resultType;
		}
		
		@Override
		public void open(Configuration conf) {
			int superstep = getIterationRuntimeContext().getSuperstepNumber();
			System.out.println("Dependency Iteration " + superstep);
		}
	
		@Override
		public void flatMap(Tuple2<Tuple2<K, V>, Tuple2<K, V>> value,
				Collector<Tuple2<K, V>> out) throws Exception {
			
			// count changed elements
			if (!(value.f0.equals(value.f1))) {
				// emit updated values only
				out.collect(value.f0);
			}
		}
		
		@Override
		public TypeInformation<Tuple2<K, V>> getProducedType() {
			return this.resultType;
		}

	}

}
