package eu.stratosphere.fixpoint.api;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.operators.Operator;
import eu.stratosphere.api.java.operators.UnsortedGrouping;
import eu.stratosphere.api.java.tuple.Tuple2;

/**
 * 
 * The FixedPointIteration represents a fixed point iteration.
 * It is created by providing a DataSet containing the vertices (parameters) and their initial values
 * and a DataSet containing the edges (dependencies) among vertices and their initial weights.
 * 
 * @param <K> The data type of the vertex keys
 * @param <V> The data type of the vertex values
 * @param <E> The data type of the edges
 * 
 */
public abstract class FixedPointIteration<K, V, E> implements StepFunctionIterative<K, V> {
	
	public FixedPointIteration(DataSet<Tuple2<K, V>> vertices, DataSet<E> edges) {
		
	}

	@Override
	public abstract Operator<Tuple2<K, V>, ?> stepFunction(UnsortedGrouping<Tuple2<K, V>> neighborsValues);
	
	/**
	 * assembles the plan(s), set the cost model convergence, sets output path, etc.
	 */
	//TODO: implement!
	public void submit(String resultPath) {
		
	}
	
	// set cost model on/off
	
	// override the default cost model? need access to number of updated elements and avg node degree 
}
