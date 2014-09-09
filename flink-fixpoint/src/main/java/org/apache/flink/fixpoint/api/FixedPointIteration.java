package org.apache.flink.fixpoint.api;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.DeltaIteration;
import org.apache.flink.api.java.IterativeDataSet;
import org.apache.flink.api.java.functions.RichFlatMapFunction;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.BasicTypeInfo;
import org.apache.flink.types.LongValue;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.Collector;

/**
 * 
 * This class represents a fixed point iteration.
 * It is created by providing a DataSet containing the parameters and their initial values,
 * a DataSet containing the dependencies among parameters with (optionally) their initial weights
 * and a step function.
 * 
 * @param <K> The data type of the parameter keys
 * @param <V> The data type of the parameter values
 * @param <E> The data type of the dependency weight
 * 
 */
public class FixedPointIteration<K, V, E> implements CustomUnaryOperation<Tuple2<K, V>, Tuple2<K, V>>{

	private DataSet<Tuple2<K, V>> parametersInput;
	private final DataSet<Tuple3<K, K, E>> dependenciesWithWeight;
	private final DataSet<Tuple2<K, K>> dependenciesWithoutWeight;
	private final int numberOfParameters;
	private final double avgNodeDegree;
	private final StepFunction<K, V, E> stepFunction;
	private final int maxIterations;
	private String name;
	private static String execMode = "COST_MODEL";	// default value
	
	private static final String UPDATED_ELEMENTS_AGGR = "updated.elements.aggr";
	private static int iterationsElapsed;
	private static long bulkUpdatedElements = 0;
	 
	
	private FixedPointIteration(DataSet<Tuple3<K, K, E>> dependenciesWithWeight, StepFunction<K, V, E> stepFunction, 
			int maxIterations, String mode, int numParameters, double avgNodeDegree) {

		Validate.notNull(dependenciesWithWeight);
		Validate.isTrue(maxIterations > 0, "The maximum number of iterations must be at least one.");
		
		// check that the dependencies are actually a valid tuple set of parameter key types
		TypeInformation<Tuple3<K, K, E>> dependenciesType = dependenciesWithWeight.getType();
		Validate.isTrue(dependenciesType.isTupleType() && dependenciesType.getArity() == 3, "The dependencies data set "
				+ "(for dependencies with weights) must consist of 3-tuples.");
		
		TupleTypeInfo<?> tupleInfo = (TupleTypeInfo<?>) dependenciesType;
		Validate.isTrue(tupleInfo.getTypeAt(0).equals(tupleInfo.getTypeAt(1)),
			"Both tuple fields (source and target parameter id) must be of the data type that represents the parameter key.");

		this.dependenciesWithWeight = dependenciesWithWeight;
		this.dependenciesWithoutWeight = null;
		this.maxIterations = maxIterations;
		this.stepFunction  = stepFunction;
		this.numberOfParameters =  numParameters; // parametersInput.count()
		this.avgNodeDegree = avgNodeDegree; // dependenciesWithInput.count() / numberOfParameters
		this.execMode = mode != null ? mode : "COST_MODEL";		

	}
	
	private FixedPointIteration(DataSet<Tuple2<K, K>> dependenciesWithoutWeight, StepFunction<K, V, E> stepFunction, 
			int maxIterations, String mode, int numParameters, double avgNodeDegree, boolean noDepepndencyWeight) {
		
		Validate.notNull(dependenciesWithoutWeight);
		Validate.isTrue(maxIterations > 0, "The maximum number of iterations must be at least one.");
		
		// check that the edges are actually a valid tuple set of vertex key types
		TypeInformation<Tuple2<K, K>> dependenciesType = dependenciesWithoutWeight.getType();
		Validate.isTrue(dependenciesType.isTupleType() && dependenciesType.getArity() == 2, "The dependencies data set "
				+ "(for dependencies without weights) must consist of 2-tuples.");
		
		TupleTypeInfo<?> tupleInfo = (TupleTypeInfo<?>) dependenciesType;
		Validate.isTrue(tupleInfo.getTypeAt(0).equals(tupleInfo.getTypeAt(1)),
			"Both tuple fields (source and target parameter id) must be of the data type that represents the parameter key.");
		
		this.dependenciesWithoutWeight = dependenciesWithoutWeight;
		this.dependenciesWithWeight = null;
		this.maxIterations = maxIterations;
		this.stepFunction  = stepFunction;
		this.numberOfParameters = numParameters; // verticesInput.count()
		this.avgNodeDegree = avgNodeDegree; // edgesInput.count() / numberOfVertices
		this.execMode = mode != null ? mode : "COST_MODEL";
	}

	
	// set cost model on/off
	
	// override the default cost model? need access to number of updated elements and avg node degree 
	
	/**
	 * Sets the input data set for this operator. In the case of this operator this input data set represents
	 * the set of parameters with their initial state.
	 * 
	 * @param inputData The input data set, which in the case of this operator represents the set of
	 *                  parameters with their initial state.
	 * 
	 * @see eu.stratosphere.api.java.operators.CustomUnaryOperation#setInput(eu.stratosphere.api.java.DataSet)
	 */
	@Override
	public void setInput(DataSet<Tuple2<K, V>> inputData) {
		// check that we really have 2-tuples
		TypeInformation<Tuple2<K, V>> inputType = inputData.getType();
		Validate.isTrue(inputType.isTupleType() && inputType.getArity() == 2, "The input data set (the initial parameters) "
				+ "must consist of 2-tuples.");

		// check that the key type here is the same as for the edges
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) inputType).getTypeAt(0);
		TypeInformation<?> dependencyType = dependenciesWithWeight != null ? dependenciesWithWeight.getType() : 
			dependenciesWithoutWeight.getType();
		TypeInformation<K> dependencyKeyType = ((TupleTypeInfo<?>) dependencyType).getTypeAt(0);
		
		Validate.isTrue(keyType.equals(dependencyKeyType), "The first tuple field (the parameter id) of the input data set "
				+ "(the initial parameters) must be the same data type as the first fields of the dependency data set "
				+ "(the source parameter id). Here, the key type for the parameter ids is '%s' and the key type  for the dependencies"
				+ " is '%s'.", keyType, dependencyKeyType);

		this.parametersInput = inputData;
		
	}

	@Override
	public DataSet<Tuple2<K, V>> createResult() {

		if (this.parametersInput == null) {
			throw new IllegalStateException("The input data set has not been set.");
		}
		
		/**
		 * Prepare type information
		 */
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) parametersInput.getType()).getTypeAt(0);
		TypeInformation<Tuple1<K>> tupleKeyType = new TupleTypeInfo<Tuple1<K>>(keyType);
		
		TypeInformation<V> valueType = ((TupleTypeInfo<?>) parametersInput.getType()).getTypeAt(1);
		
		TypeInformation<?>[] parameterTypes = {(BasicTypeInfo<?>)keyType, valueType};
		TypeInformation<Tuple2<K, V>> parameterTypeInfo = new TupleTypeInfo<Tuple2<K,V>>(parameterTypes);
		
		TypeInformation<?> dependencyType = dependenciesWithWeight != null ? dependenciesWithWeight.getType() : 
			dependenciesWithoutWeight.getType();
		
		TypeInformation<?>[] stepFunctionTypesWithoutWeight = {(BasicTypeInfo<?>)keyType, (BasicTypeInfo<?>)keyType, valueType}; 
		TypeInformation<Tuple3<K, K, V>> stepFunctionInputTypeWithoutWeight = new TupleTypeInfo<Tuple3<K,K,V>>(stepFunctionTypesWithoutWeight);
		
		TypeInformation<E> weightType = null;
		if (dependenciesWithWeight != null) {
			weightType = ((TupleTypeInfo<?>) dependenciesWithWeight.getType()).getTypeAt(2);
		}
		
		TypeInformation<?>[] stepFunctionTypesWithWeight = {(BasicTypeInfo<?>)keyType, (BasicTypeInfo<?>)keyType, valueType, (BasicTypeInfo<?>)weightType};
		TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputTypeWithWeight = new TupleTypeInfo<Tuple4<K,K,V,E>>(stepFunctionTypesWithWeight);
		

		final String name = (this.name != null) ? this.name :
			"Fixpoint iteration (" + stepFunction + ")";
		
		/**
		 * Check whether the execution type has been defined
		 */
		switch (execMode) {
		case "BULK":
			DataSet<Tuple2<K, V>> bulkResult = doBulkIteration(name, stepFunctionInputTypeWithWeight, stepFunctionInputTypeWithoutWeight,
					parameterTypeInfo);
			return bulkResult;
		case "INCREMENTAL":
			DataSet<Tuple2<K, V>> incrResult = doIncrementalIteration(name, stepFunctionInputTypeWithWeight, stepFunctionInputTypeWithoutWeight,
					parameterTypeInfo);
			return incrResult;
		case "DELTA":
			DataSet<Tuple2<K, V>> deltaResult = doDeltaIteration(name, stepFunctionInputTypeWithWeight, stepFunctionInputTypeWithoutWeight,
					parameterTypeInfo);
			return deltaResult;
		case "COST_MODEL":
			/**
			 * Start with a bulk iteration
			 */
			
			DataSet<Tuple2<K, V>> bulkIntermediate = doBulkIteration(name, stepFunctionInputTypeWithWeight, stepFunctionInputTypeWithoutWeight,
					parameterTypeInfo);
			
			// TODO: check whether there are iterations left
			
			/**
			 * TODO: If there are no elements changed during the last bulk iteration,
			 * we shouldn't execute any dependency iteration
			 */
				
			DataSet<Tuple2<K, V>> depResult = doDependencyIteration(name, bulkIntermediate, tupleKeyType, dependencyType, 
					stepFunctionInputTypeWithWeight, stepFunctionInputTypeWithoutWeight, parameterTypeInfo);
			return depResult;
		default:
			throw new IllegalArgumentException("Unkown execution mode " + execMode);
		}
		
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> doBulkIteration(String name, TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputTypeWithWeight, 
			TypeInformation<Tuple3<K, K, V>> stepFunctionInputTypeWithoutWeight, 
			TypeInformation<Tuple2<K, V>> parameterTypeInfo) {
		
		// set up the iteration operator
		IterativeDataSet<Tuple2<K, V>> iteration = parametersInput.iterate(maxIterations);
		iteration.name(name);
		
		// register convergence criterion
		if (execMode.equals("COST_MODEL")) {
			iteration.registerAggregationConvergenceCriterion(UPDATED_ELEMENTS_AGGR, new LongSumAggregator(), 
					new UpdatedElementsCostModelConvergence(numberOfParameters, avgNodeDegree));
		}
		
		DataSet<Tuple2<K, V>> parametersWithNewValues;
		
		if (dependenciesWithWeight != null) {
			parametersWithNewValues = getBulkResultWithWeight(iteration, stepFunctionInputTypeWithWeight);
		}
		else {
			parametersWithNewValues = getBulkResultWithoutWeight(iteration, stepFunctionInputTypeWithoutWeight);
		}
				
		// compare with previous values
		if (execMode.equals("COST_MODEL")) {
			FlatMapOperator<?, Tuple2<K, V>> updatedParameters = parametersWithNewValues.join(iteration)
													.where(0).equalTo(0)
													.flatMap(new AggregateAndEmitUpdatedValue(parameterTypeInfo));
			// close the iteration
			return iteration.closeWith(updatedParameters);
		}
		else {
			return iteration.closeWith(parametersWithNewValues);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> doDependencyIteration(String name, DataSet<Tuple2<K, V>> bulkResult, TypeInformation<Tuple1<K>> tupleKeyType, TypeInformation<?> dependencyType, TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputTypeWithWeight, TypeInformation<Tuple3<K, K, V>> stepFunctionInputTypeWithoutWeight, TypeInformation<Tuple2<K, V>> parameterTypeInfo) {
		DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> depIteration = bulkResult.iterateDelta(bulkResult, 
				maxIterations - iterationsElapsed + 1, 0);
		depIteration.name("Dependency iteration:" + name);
		
		DataSet<Tuple2<K, V>> dependencyParametersWithNewValues;
		
		if (dependenciesWithWeight != null) {
			dependencyParametersWithNewValues = getDepResultWithWeight(depIteration, tupleKeyType, 
					(TypeInformation<Tuple3<K, K, E>>) dependencyType, stepFunctionInputTypeWithWeight);
		}
		else {
			dependencyParametersWithNewValues = getDepResultWithoutWeight(depIteration, tupleKeyType, 
					(TypeInformation<Tuple2<K, K>>) dependencyType, stepFunctionInputTypeWithoutWeight);
		}
		
		// compare with previous values
		FlatMapOperator<?, Tuple2<K, V>> dependencyUpdatedParameters = dependencyParametersWithNewValues
												.join(depIteration.getSolutionSet())
												.where(0).equalTo(0)
												.flatMap(new EmitOnlyUpdatedValues(parameterTypeInfo));
		// close the iteration
		DataSet<Tuple2<K, V>> result = depIteration.closeWith(dependencyUpdatedParameters, dependencyUpdatedParameters);
		
		return result;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> doIncrementalIteration(String name, TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputTypeWithWeight, 
			TypeInformation<Tuple3<K, K, V>> stepFunctionInputTypeWithoutWeight, 
			TypeInformation<Tuple2<K, V>> parameterTypeInfo) {
		
		// set up the iteration operator
		DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> iteration = parametersInput.iterateDelta(parametersInput, maxIterations, 0);
		iteration.name(name);
		
		DataSet<Tuple2<K, V>> parametersWithNewValues;
		
		if (dependenciesWithWeight != null) {
			parametersWithNewValues = getIncrementalResultWithWeight(iteration, stepFunctionInputTypeWithWeight);
		}
		else {
			parametersWithNewValues = getIncrementalResultWithoutWeight(iteration, stepFunctionInputTypeWithoutWeight);
		}
				
		// compare with previous values
		FlatMapOperator<?, Tuple2<K, V>> updatedParameters = parametersWithNewValues.join(iteration.getSolutionSet())
												.where(0).equalTo(0)
												.flatMap(new EmitOnlyUpdatedValues(parameterTypeInfo));
		// close the iteration
		DataSet<Tuple2<K, V>> incrementalResult = iteration.closeWith(updatedParameters, updatedParameters);
		return incrementalResult;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> doDeltaIteration(String name, TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputTypeWithWeight, 
			TypeInformation<Tuple3<K, K, V>> stepFunctionInputTypeWithoutWeight, 
			TypeInformation<Tuple2<K, V>> parameterTypeInfo) {
		
		/** 
		 * start with one bulk iteration
		 * to initialize the delta input
		 */
		IterativeDataSet<Tuple2<K, V>> bulkIteration = parametersInput.iterate(1);
		bulkIteration.name(name);
		
		DataSet<Tuple2<K, V>> parametersWithNewValues;
		
		if (dependenciesWithWeight != null) {
			parametersWithNewValues = getBulkResultWithWeight(bulkIteration, stepFunctionInputTypeWithWeight);
		}
		else {
			parametersWithNewValues = getBulkResultWithoutWeight(bulkIteration, stepFunctionInputTypeWithoutWeight);
		}
				
		// close the bulk iteration
		DataSet<Tuple2<K, V>> bulkIntermediate = bulkIteration.closeWith(parametersWithNewValues);
		
		/**
		 *  set up the delta iteration operator
		 */
		// initial workset
		DataSet<Tuple2<K, V>> deltasInput = stepFunction.deltaInput(parametersInput, bulkIntermediate);
		
		DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> iteration = parametersInput.iterateDelta(deltasInput, maxIterations - 1, 0);
		iteration.name(name);
		
		DataSet<Tuple2<K, V>> deltaParametersWithNewValues;
		
		if (dependenciesWithWeight != null) {
			deltaParametersWithNewValues = getDeltaResultWithWeight(iteration, stepFunctionInputTypeWithWeight);
		}
		else {
			deltaParametersWithNewValues = getDeltaResultWithoutWeight(iteration, stepFunctionInputTypeWithoutWeight);
		}
				
		// compare with previous values
		FlatMapOperator<?, Tuple2<K, V>> deltaUpdatedParameters = deltaParametersWithNewValues.join(iteration.getSolutionSet())
												.where(0).equalTo(0)
												.flatMap(new EmitDeltaUpdatedValues(parameterTypeInfo, this.stepFunction));
		// close the iteration
		DataSet<Tuple2<K, V>> deltaResult = iteration.closeWith(deltaUpdatedParameters, deltaParametersWithNewValues);
		return deltaResult;
	}


	private DataSet<Tuple2<K, V>> getBulkResultWithoutWeight(
			IterativeDataSet<Tuple2<K, V>> iteration, TypeInformation<Tuple3<K, K, V>> stepFunctionInputType) {
		// TODO make StepFunction work for dependencies without weight
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> getBulkResultWithWeight(IterativeDataSet<Tuple2<K, V>> iteration, 
			TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputType) {
		
		// produce the DataSet containing each vertex with the in-neighbor and their value		
		FlatMapOperator<?, Tuple4<K, K, V, E>> parametersWithNeighborValues = 
				iteration.join(dependenciesWithWeight)
				.where(0).equalTo(0).flatMap(new ProjectStepFunctionInput(stepFunctionInputType));
		
		// result of the step function
		return this.stepFunction.updateState(parametersWithNeighborValues, iteration);
	}
	
	private DataSet<Tuple2<K, V>> getDepResultWithoutWeight(
			DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> depIteration,
			TypeInformation<Tuple1<K>> tupleKeyType, TypeInformation<Tuple2<K, K>> dependencyType, 
			TypeInformation<Tuple3<K, K, V>> stepFunctionInputType) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> getDepResultWithWeight(
			DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> depIteration,
			TypeInformation<Tuple1<K>> tupleKeyType, TypeInformation<Tuple3<K, K, E>> dependencyType, 
			TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputType) {
		
		FlatMapOperator<?, Tuple1<K>> candidates = depIteration.getWorkset().join(dependenciesWithWeight)
																.where(0).equalTo(0).flatMap(new CandidateIDs(tupleKeyType));
		
		DataSet<Tuple3<K, K, E>> candidatesDependencies = candidates.distinct().join(dependenciesWithWeight).where(0).equalTo(1)
																	.flatMap(new CandidatesDependencies(dependencyType));
		
		// produce the DataSet containing each parameter with the in-neighbor and their value		
		FlatMapOperator<?, Tuple4<K, K, V, E>> parametersWithNeighborValues = 
				depIteration.getSolutionSet().join(candidatesDependencies)
				.where(0).equalTo(0).flatMap(new ProjectStepFunctionInput(stepFunctionInputType));
		
		// result of the step function
		return this.stepFunction.updateState(parametersWithNeighborValues, depIteration.getSolutionSet());
	}
	
	private DataSet<Tuple2<K, V>> getIncrementalResultWithoutWeight(
			DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> iteration, TypeInformation<Tuple3<K, K, V>> stepFunctionInputType) {
		// TODO make StepFunction work for dependencies without weight
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> getIncrementalResultWithWeight(DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> iteration, 
			TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputType) {
		
		// produce the DataSet containing each vertex with the in-neighbor and their value		
		FlatMapOperator<?, Tuple4<K, K, V, E>> parametersWithNeighborValues = 
				iteration.getWorkset().join(dependenciesWithWeight)
				.where(0).equalTo(0).flatMap(new ProjectStepFunctionInput(stepFunctionInputType));
		
		// result of the step function
		return this.stepFunction.updateState(parametersWithNeighborValues, iteration.getSolutionSet());
	}
	
	private DataSet<Tuple2<K, V>> getDeltaResultWithoutWeight(
			DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> iteration, TypeInformation<Tuple3<K, K, V>> stepFunctionInputType) {
		// TODO make StepFunction work for dependencies without weight
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private DataSet<Tuple2<K, V>> getDeltaResultWithWeight(DeltaIteration<Tuple2<K, V>, Tuple2<K, V>> iteration, 
			TypeInformation<Tuple4<K, K, V, E>> stepFunctionInputType) {
		
		// produce the DataSet containing each vertex with the in-neighbor and their value		
		FlatMapOperator<?, Tuple4<K, K, V, E>> parametersWithNeighborValues = 
				iteration.getWorkset().join(dependenciesWithWeight)
				.where(0).equalTo(0).flatMap(new ProjectStepFunctionInput(stepFunctionInputType));
		
		// result of the step function
		return this.stepFunction.updateState(parametersWithNeighborValues, iteration.getSolutionSet());
	}

	/**
	 * Creates a new fixed point iteration operator for dependency graphs where the edges are not associated with a weight.
	 * 
	 * @param dependenciesWithoutWeight The data set containing the dependencies in the form of edges. Edges are represented as 2-tuples: (source-id, target-id)
	 * @param stepFunction The step function that updates the state of the parameters from the states of the in-neighbors.
	 * 
	 * @param <K> The type of the parameter key (the parameter identifier).
	 * @param <V> The type of the parameter value (the state of the parameter).
	 * 
	 * @return An in stance of the fixed point computation operator.
	 */
	public static final <K, V> FixedPointIteration<K, V, Object> withPlainDependencies(
					DataSet<Tuple2<K, K>> dependenciesWithoutWeight,
						StepFunction<K, V, Object> stepFunction,
						int maximumNumberOfIterations, String mode, int numParameters, double avgNodeDegree)
	{		
		return new FixedPointIteration<K, V, Object>(dependenciesWithoutWeight, stepFunction, 
				maximumNumberOfIterations, mode, numParameters, avgNodeDegree, true);
	}
	
	/**
	 * Creates a new fixed point iteration operator for graphs where the dependencies are associated with a weight.
	 * 
	 * @param dependenciesWithValue The data set containing the dependencies in the form of edges. Edges are represented as 3-tuples: (source-id, target-id, weight)
	 * @param stepFunction The step function that updates the state of the parameters from the states of the in-neighbors.
	 * 
	 * @param <K> The type of the parameter key (the parameter identifier).
	 * @param <V> The type of the parameter value (the state of the parameter).
	 * @param <E> The type of the weight associated with the dependencies.
	 * 
	 * @return An in stance of the fixed point computation operator.
	 */
	public static final <K, V, E> FixedPointIteration<K, V, E> withWeightedDependencies(
					DataSet<Tuple3<K, K, E>> dependenciesWithWeight,
					StepFunction<K, V, E> stepFunction,
					int maximumNumberOfIterations, String mode, int numParameters, double avgNodeDegree)
	{
		return new FixedPointIteration<K, V, E>(dependenciesWithWeight, stepFunction, maximumNumberOfIterations, mode,
				numParameters, avgNodeDegree);
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
	
	private static final class ProjectStepFunctionInput<K, V, E> implements FlatMapFunction
		<Tuple2<Tuple2<K, V>, Tuple3<K, K, E>>, Tuple4<K, K, V, E>>, ResultTypeQueryable<Tuple4<K, K, V, E>> {
		
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
	 * the dependencies set should contain a self-dependency for every parameter
	 *
	 */
	private static final class AggregateAndEmitUpdatedValue<K, V> extends RichFlatMapFunction
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
			if (execMode.equals("COST_MODEL")) {
				updatedElementsAggr = getIterationRuntimeContext().getIterationAggregator(UPDATED_ELEMENTS_AGGR); 
				iterationsElapsed = getIterationRuntimeContext().getSuperstepNumber();
				if (iterationsElapsed > 1) {
					LongValue updatedElementsValue = getIterationRuntimeContext().getPreviousIterationAggregate(UPDATED_ELEMENTS_AGGR);
					bulkUpdatedElements = updatedElementsValue.getValue();
					System.out.println("Updated elements: " +bulkUpdatedElements);
				}
				System.out.println("Bulk Iteration " + iterationsElapsed);
			}
		}

		@Override
		public void flatMap(Tuple2<Tuple2<K, V>, Tuple2<K, V>> value,
				Collector<Tuple2<K, V>> out) throws Exception {
			
			if (execMode.equals("COST_MODEL")) {
				// count changed elements
				if (!(value.f0.equals(value.f1))) {
					updatedElementsAggr.aggregate(1);
				}
			}
			// emit the updated value
			out.collect(value.f0);
		}
		
		@Override
		public TypeInformation<Tuple2<K, V>> getProducedType() {
			return this.resultType;
		}

	}
	
	private static final class CandidateIDs<K, V, E> implements FlatMapFunction
		<Tuple2<Tuple2<K, V>, Tuple3<K, K, E>>, Tuple1<K>>, ResultTypeQueryable<Tuple1<K>> {
		
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
	
	private static final class CandidatesDependencies<K, E> implements FlatMapFunction
		<Tuple2<Tuple1<K>, Tuple3<K, K, E>>, Tuple3<K, K, E>>, ResultTypeQueryable<Tuple3<K, K, E>> {
	
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

	private static final class EmitOnlyUpdatedValues<K, V> implements FlatMapFunction
		<Tuple2<Tuple2<K, V>, Tuple2<K, V>>, Tuple2<K, V>>, ResultTypeQueryable<Tuple2<K, V>> {
	
		private static final long serialVersionUID = 1L;
		private transient TypeInformation<Tuple2<K, V>> resultType;
		
		private EmitOnlyUpdatedValues(TypeInformation<Tuple2<K, V>> resultType)
		{
			this.resultType = resultType;
		}
	
		@Override
		public void flatMap(Tuple2<Tuple2<K, V>, Tuple2<K, V>> value,
				Collector<Tuple2<K, V>> out) throws Exception {
			
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

	private static final class EmitDeltaUpdatedValues<K, V> implements FlatMapFunction
		<Tuple2<Tuple2<K, V>, Tuple2<K, V>>, Tuple2<K, V>>, ResultTypeQueryable<Tuple2<K, V>> {

		private static final long serialVersionUID = 1L;
		private transient TypeInformation<Tuple2<K, V>> resultType;
		private final StepFunction<K, V, ?> stepFunction;
		
		private EmitDeltaUpdatedValues(TypeInformation<Tuple2<K, V>> resultType, StepFunction<K, V, ?> stepFunction)
		{
			this.resultType = resultType;
			this.stepFunction = stepFunction;
		}
	
		@Override
		public void flatMap(Tuple2<Tuple2<K, V>, Tuple2<K, V>> value,
				Collector<Tuple2<K, V>> out) throws Exception {
			
			Tuple2<K, V> newValue = stepFunction.deltaUpdate(value.f1, value.f0);
			
			if (!(stepFunction.deltaEquals(newValue, value.f1))) {
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
