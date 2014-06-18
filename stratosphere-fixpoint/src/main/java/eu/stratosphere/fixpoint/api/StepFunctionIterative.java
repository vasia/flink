package eu.stratosphere.fixpoint.api;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple4;

/**
 *
 * @param <K> The data type of the vertex keys
 * @param <V> The data type of the vertex values
 * @param <E> The data type of the edge value
 */
public interface StepFunctionIterative<K, V, E> {
	
	/**
	 * 
	 * @param inComingNeighbors: <trgID, srcID, srcValue, edgeValue> 
	 * @return: <trgID, newValue>
	 */
	DataSet<Tuple2<K, V>> stepFunction(DataSet<Tuple4<K, K, V, E>> inNeighbors);

}
