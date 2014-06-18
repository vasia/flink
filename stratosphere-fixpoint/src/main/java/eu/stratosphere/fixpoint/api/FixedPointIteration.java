package eu.stratosphere.fixpoint.api;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;

/**
 * 
 * The FixedPointIteration represents a fixed point iteration.
 * It is created by providing a DataSet containing the vertices (parameters) and their initial values
 * and a DataSet containing the edges (dependencies) among vertices and their initial weights.
 * 
 * @param <K> The data type of the vertex keys
 * @param <V> The data type of the vertex values
 * @param <E> The data type of the edge value
 * 
 */
public abstract class FixedPointIteration<K, V, E> implements StepFunctionIterative<K, V, E> {
	
	private DataSet<Tuple2<K, V>> verticesInput;
	private DataSet<Tuple3<K, K, E>> edgesInput;
	
	public FixedPointIteration(DataSet<Tuple2<K, V>> vertices, DataSet<Tuple3<K, K, E>> edges) {
		this.verticesInput = vertices;
		this.edgesInput = edges;
	}

	
	/**
	 * assembles the plan(s), set the cost model convergence, sets output path, etc.
	 */
	//TODO: implement!
	public void submit(String resultPath) {
		
	}


	@Override
	public abstract DataSet<Tuple2<K, V>> stepFunction(DataSet<Tuple4<K, K, V, E>> inNeighbors);
	
	// set cost model on/off
	
	// override the default cost model? need access to number of updated elements and avg node degree 
}
