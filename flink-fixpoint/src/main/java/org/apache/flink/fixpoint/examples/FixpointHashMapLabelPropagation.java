package org.apache.flink.fixpoint.examples;

import java.util.HashMap;
import java.util.Random;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fixpoint.api.FixedPointIteration;
import org.apache.flink.fixpoint.api.StepFunction;
import org.apache.flink.util.Collector;



public class FixpointHashMapLabelPropagation implements ProgramDescription {

	public static void main(String... args) throws Exception {
		
		if (args.length < 7) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <number-of-labels> "
					+ "<max-iterations> <execution-mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>"
					+ " <numParameters> <avg-node-degree>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		final int numLabels = Integer.parseInt(args[3]);
		final int maxIterations = Integer.parseInt(args[4]);
		final int numParameters = Integer.parseInt(args[6]);
		final double avgNodeDegree = Double.parseDouble(args[7]);

		
		// initialize vertices with random labels
		@SuppressWarnings("serial")
		DataSet<Tuple2<Long, Integer>> vertices = env.readTextFile(args[0]).map(
				new MapFunction<String, Tuple2<Long, Integer>>() {
					public Tuple2<Long, Integer> map(String value)
							throws Exception {
						Random randomGenerator = new Random();
						return new Tuple2<Long, Integer>(Long.parseLong(value), randomGenerator.nextInt(numLabels));
					}
				}) ;
		
		DataSet<Tuple3<Long, Long, Double>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class, 
				Double.class); 
	
		DataSet<Tuple2<Long, Integer>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new MostFrequentLabel(), maxIterations, args[5], numParameters, avgNodeDegree));
		result.print();
		env.execute("Fixed Point Label Propagation (HashMap Implementation)");
		
	}
	
	@SuppressWarnings("serial")
	public static final class MostFrequentLabel extends StepFunction<Long, Integer, Double> {

		@Override
		public DataSet<Tuple2<Long, Integer>> updateState(
				DataSet<Tuple4<Long, Long, Integer, Double>> inNeighbors,
				DataSet<Tuple2<Long, Integer>> state) {
			
			return inNeighbors.groupBy(0).reduceGroup(new FindMostFrequentLabel());
		}
	}
	
	@SuppressWarnings("serial")
	public static final class FindMostFrequentLabel extends RichGroupReduceFunction<Tuple4<Long, Long, Integer, Double>, 
		Tuple2<Long, Integer>> {
		
		private HashMap<Integer, Double> labelsWithFrequencies = new HashMap<Integer, Double>();
		private Long vertexID;
		private Integer firstLabel;
		
		@Override
		public void open(Configuration conf) {
			labelsWithFrequencies.clear();
		}

		@Override
		public void reduce(
				Iterable<Tuple4<Long, Long, Integer, Double>> values,
				Collector<Tuple2<Long, Integer>> out) throws Exception {
			
			int i = 0;
				for (Tuple4<Long, Long, Integer, Double> rec : values) { 
					if (i == 0) {
						// store vertexID and first label
						vertexID = rec.f0;
						firstLabel = rec.f2;
			   			labelsWithFrequencies.put(firstLabel, rec.f3);						
					}
					else {
						// process the rest of the records
						Integer currentLabel = rec.f2;
						
						if (!(labelsWithFrequencies.containsKey(currentLabel))) {
			    			// first time we see this label
			    			labelsWithFrequencies.put(currentLabel, rec.f3);
			    		}
			    		else {
			    			// we have seen this label before -- add weight
			    			double freq = labelsWithFrequencies.get(currentLabel).doubleValue();
			    			labelsWithFrequencies.put(currentLabel, new Double(freq + rec.f3.doubleValue()));
			    		}
					}
					i++;
				}
				
				if (i > 0) {
					// adopt the most frequent label
					double maxFreq = 0.0;
			    	Integer mostFrequentLabel = firstLabel;
			    	
			    	for (Integer label: labelsWithFrequencies.keySet()) {
			    		if (labelsWithFrequencies.get(label).doubleValue() > maxFreq) {
			    			maxFreq = labelsWithFrequencies.get(label).doubleValue();
			    			mostFrequentLabel = label;
			    		}
			    	}
			    	
					out.collect(new Tuple2<Long, Integer>(vertexID, mostFrequentLabel));
				}
			}
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <number-of-labels> <max-number-of-iterations> "
				+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>"
				+ "<numParameters> <avg-node-degree>";
	}
	
}
