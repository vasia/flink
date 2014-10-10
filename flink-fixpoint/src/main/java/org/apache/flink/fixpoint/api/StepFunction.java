package org.apache.flink.fixpoint.api;

import java.io.Serializable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 *
 * @param <K> The data type of the vertex keys
 * @param <V> The data type of the vertex values
 * @param <E> The data type of the edge value
 */
public abstract class StepFunction<K, V, E>  implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Collector<Tuple2<K, V>> out;
	
	private Tuple2<K, V> outVal;
	
	void setOutput(Tuple2<K, V> val, Collector<Tuple2<K, V>> out) {
		this.out = out;
		this.outVal = val;
	}

	/**
	 * 
	 * @param inComingNeighbors: <trgID, srcID, srcValue, edgeValue> 
	 * @return: <trgID, newValue>
	 */
	public abstract DataSet<Tuple2<K, V>> updateState(DataSet<Tuple4<K, K, V, E>> inNeighbors, 
			DataSet<Tuple2<K, V>> state);
	
	/**
	 * 
	 * This method needs to be overriden when the delta execution mode is chosen.
	 * It defines how to produce the input to the delta iteration plan.
	 * @param input: input to the first bulk iteration
	 * @param resultAfterBulk: result of the first bulk iteration 
	 * @return: the input to the delta iteration
	 */
	public DataSet<Tuple2<K, V>> deltaInput(DataSet<Tuple2<K, V>> input, DataSet<Tuple2<K, V>> resultAfterBulk) {
		throw new UnsupportedOperationException("This method needs to be overriden, "
				+ "when using the delta execution mode.");
	}

	/**
	 * 
 	 * This method needs to be overriden when the delta execution mode is chosen.
	 * It defines how to produce the next solution set.
	 * @param previousValue: the previous value of the solution set
	 * @param deltaValue: the delta value computed in the current iteration
	 * @return: the new value of the solution set
	 */
	public Tuple2<K, V> deltaUpdate(Tuple2<K, V> previousValue, Tuple2<K, V> deltaValue) {
		throw new UnsupportedOperationException("This method needs to be overriden, "
				+ "when using the delta execution mode.");
	}
	
	/**
	 * 
	 * This method needs to be overriden when the delta execution mode is chosen.
	 * It defines when two values are considered equal, at the end of a delta iteration.
	 * @param previousValue
	 * @param currentValue
	 * @return true if equal.
	 */
	public boolean deltaEquals(Tuple2<K, V> previousValue, Tuple2<K, V> currentValue) {
		throw new UnsupportedOperationException("This method needs to be overriden, "
				+ "when using the delta execution mode.");
	}
	
	/**
	 * Sets the new value of this vertex.
	 * 
	 * @param newValue The new vertex value.
	 */
	public void setNewVertexValue(V newValue) {
		outVal.f1 = newValue;
		out.collect(outVal);
	}	

}
