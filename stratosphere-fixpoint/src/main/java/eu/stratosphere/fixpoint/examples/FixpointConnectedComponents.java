package eu.stratosphere.fixpoint.examples;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.fixpoint.api.FixedPointIteration;
import eu.stratosphere.types.NullValue;

public class FixpointConnectedComponents {

	public static void main(String... args) throws Exception {
		
		if (args.length < 3) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> vertices = env.readCsvFile(args[0]).types(Long.class, Long.class);
		
		DataSet<Tuple3<Long, Long, NullValue>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class, NullValue.class);
		
		FixedPointIteration<Long, Long, NullValue> cc = new FixedPointIteration<Long, Long, NullValue>(vertices, edges) {

			@Override
			public DataSet<Tuple2<Long, Long>> stepFunction(DataSet<Tuple4<Long, Long, Long, NullValue>> inNeighbors) {
				return inNeighbors.groupBy(0).aggregate(Aggregations.MIN, 2).project(0, 2).types(Long.class, Long.class);
			}			
		};
		
		cc.submit(args[2]);
	}
	
}
