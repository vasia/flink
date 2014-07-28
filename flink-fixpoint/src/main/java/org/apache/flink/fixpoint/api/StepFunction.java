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
	public abstract DataSet<Tuple2<K, V>> updateState(DataSet<Tuple4<K, K, V, E>> inNeighbors);
	
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
