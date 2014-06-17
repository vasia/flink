package eu.stratosphere.fixpoint.api;

import eu.stratosphere.api.java.operators.Operator;
import eu.stratosphere.api.java.operators.UnsortedGrouping;
import eu.stratosphere.api.java.tuple.Tuple2;

/**
 *
 * @param <T> The data type of the vertex values
 */
public interface StepFunctionIterative<K, V> {
	
	Operator<Tuple2<K, V>, ?> stepFunction(UnsortedGrouping<Tuple2<K, V>> neighborsValues);

}
