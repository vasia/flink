package org.apache.flink.fixpoint.examples;

import java.util.HashMap;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fixpoint.api.FixedPointIteration;
import org.apache.flink.fixpoint.api.StepFunction;
import org.apache.flink.fixpoint.util.ExecutionMode;
import org.apache.flink.util.Collector;

public class FixpointHashMapCommunityDetection implements ProgramDescription {

	private static double delta;

	@SuppressWarnings("serial")
	public static void main(String... args) throws Exception {
		
		if (args.length < 7) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <delta> <max_iterations>"
					+ " <numParameters> <avg-node-degree>"
					+ " <execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>");
			return;
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// <vertexID, <label, score>>
		DataSet<Tuple2<Long, Tuple2<Long, Double>>> vertices = env.readCsvFile(args[0]).fieldDelimiter('\t').types(Long.class, 
				Long.class, Double.class).map(new MapFunction<Tuple3<Long, Long, Double>, 
						Tuple2<Long, Tuple2<Long, Double>>>(){
					public Tuple2<Long, Tuple2<Long, Double>> map(
							Tuple3<Long, Long, Double> value) throws Exception {
						return new Tuple2<Long, Tuple2<Long, Double>>(value.f0, new Tuple2<Long, Double>(value.f1, value.f2));
					}
					
				});
		
		DataSet<Tuple3<Long, Long, Double>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class, 
				Double.class); 
		
		delta = Double.parseDouble(args[3]);
		final int maxIterations = Integer.parseInt(args[4]);
		final int numParameters = Integer.parseInt(args[5]);
		final double avgNodeDegree = Double.parseDouble(args[6]);

	
		DataSet<Tuple2<Long, Tuple2<Long, Double>>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new ComputeCommunities(), maxIterations, ExecutionMode.parseExecutionModeArgs(args[7]), 
				numParameters, avgNodeDegree));

		result.writeAsText(args[2]);
		env.execute("Fixed Point Community Detection (HashMap implementation)");
		
	}
	
	@SuppressWarnings("serial")
	public static final class ComputeCommunities extends StepFunction<Long, Tuple2<Long, Double>, Double> {

		@Override
		public DataSet<Tuple2<Long, Tuple2<Long, Double>>> updateState(
			DataSet<Tuple4<Long, Long, Tuple2<Long, Double>, Double>> inNeighbors, 
			DataSet<Tuple2<Long, Tuple2<Long, Double>>> state) {
			
			// <vertexID, neighborID, neighborLabel, neighborScore, edgeWeight>
			DataSet<Tuple5<Long, Long, Long, Double, Double>> flattenedNeighborWithLabel = inNeighbors.map(new FlattenNeighbors());
			DataSet<Tuple2<Long, Tuple2<Long, Double>>> verticesWithNewScoredLabels = flattenedNeighborWithLabel.groupBy(0)
					.reduceGroup(new ScorerReducer()).join(state).where(0).equalTo(0).map(new NewScoreMapper());
					
					
			return verticesWithNewScoredLabels;
		}
		
	}
	
	public static final class FlattenNeighbors implements MapFunction<Tuple4<Long, Long, Tuple2<Long, Double>, Double>, 
		Tuple5<Long, Long, Long, Double, Double>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple5<Long, Long, Long, Double, Double> map(Tuple4<Long, Long, Tuple2<Long, Double>, Double> value)
				throws Exception {
			return new Tuple5<Long, Long, Long, Double, Double>
				(value.f0, value.f1, value.f2.f0, value.f2.f1, value.f3);
		}
	}
	
	public static final class ScorerReducer extends RichGroupReduceFunction<Tuple5<Long, Long, Long, Double, Double>, 
		Tuple3<Long, Long, Double>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private Long vertexID;
		private Long firstLabel;
		private HashMap<Long, Double> receivedLabelsWithScores = new HashMap<Long, Double>();
		private HashMap<Long, Double> labelsWithHighestScore = new HashMap<Long, Double>();
		
		@Override
		public void open(Configuration conf) {
			receivedLabelsWithScores.clear();
			labelsWithHighestScore.clear();
		}
		
		@Override
		public void reduce(
				// <vertexID, neighborID, neighborLabel, neighborScore, edgeWeight>
				Iterable<Tuple5<Long, Long, Long, Double, Double>> values,
				Collector<Tuple3<Long, Long, Double>> out)	throws Exception {
			
			int i = 0;
			for (Tuple5<Long, Long, Long, Double, Double> rec : values) { 
				if (i == 0) {
					// store vertexID and first label
					vertexID = rec.f0;
					firstLabel = rec.f2;
					receivedLabelsWithScores.put(firstLabel, rec.f3);		
					labelsWithHighestScore.put(firstLabel, rec.f3);
				}
				else {
					// process the rest of the records
					Long currentLabel = rec.f2;
					Double receivedScore = rec.f3;
					
					if (!(receivedLabelsWithScores.containsKey(currentLabel))) {
		    			// first time we see this label
						receivedLabelsWithScores.put(currentLabel, receivedScore);
		    		}
		    		else {
		    			// we have seen this label before -- add score
		    			double freq = receivedLabelsWithScores.get(currentLabel).doubleValue();
		    			receivedLabelsWithScores.put(currentLabel, new Double(freq + receivedScore.doubleValue()));
		    		}
					
					// store the highest score for each received label
					if (labelsWithHighestScore.containsKey(currentLabel)) {
						double currentScore = labelsWithHighestScore.get(currentLabel);
						if (currentScore < receivedScore) {
							// record the highest score
							labelsWithHighestScore.put(currentLabel, receivedScore);
						}
					}
					else {
						// first time we see this label
						labelsWithHighestScore.put(currentLabel, receivedScore);
					}
				}
				i++;
			} // all labels have been processed
			
			if (i > 0) {
				// find the label with the highest score from the ones received
				double maxScore = -Double.MAX_VALUE;
				long maxScoreLabel = firstLabel;
				for (long curLabel : receivedLabelsWithScores.keySet()) {
					if (receivedLabelsWithScores.get(curLabel) > maxScore) {
						maxScore = receivedLabelsWithScores.get(curLabel);
						maxScoreLabel = curLabel;
					}
				}
				// find the highest score of maxScoreLabel in labelsWithHighestScore
				// the edge data set always contains self-edges for every node
				double highestScore = labelsWithHighestScore.get(maxScoreLabel);
				
				// emit <vertexId, maxScoreLabel, highestScore>
				out.collect(new Tuple3<Long, Long, Double>(vertexID, maxScoreLabel, highestScore));
			}
		}

		
		
	}
	
	public static final class FlattenState implements MapFunction<Tuple2<Long, Tuple2<Long, Double>>, 
		Tuple3<Long, Long, Double>> {
	
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Long, Long, Double> map(Tuple2<Long, Tuple2<Long, Double>> value)
				throws Exception {
			return new Tuple3<Long, Long, Double>
				(value.f0, value.f1.f0, value.f1.f1);
		}
	}
	
	public static final class NewScoreMapper extends RichMapFunction<Tuple2<Tuple3<Long, Long, Double>, 
		Tuple2<Long, Tuple2<Long, Double>>>, Tuple2<Long, Tuple2<Long, Double>>>{
		
		private static final long serialVersionUID = 1L;
		private int superstep;
		
		@Override
		public void open(Configuration conf){
			superstep = getIterationRuntimeContext().getSuperstepNumber();
		}
		
		@Override
		public Tuple2<Long, Tuple2<Long, Double>> map(Tuple2<Tuple3<Long, Long, Double>, 
				Tuple2<Long, Tuple2<Long, Double>>> value) throws Exception {
			
			// if the newly assigned label is the same as the previous one, 
			// delta = 0
			if (value.f0.f1 == value.f1.f1.f0) {
				return new Tuple2<Long, Tuple2<Long, Double>>(
						value.f0.f0, 
						new Tuple2<Long, Double>(value.f0.f1, value.f0.f2));
			}
			else {
				return new Tuple2<Long, Tuple2<Long, Double>>(
					value.f0.f0, 
					new Tuple2<Long, Double>(value.f0.f1, value.f0.f2 - (delta / (double) superstep)));
			}
		}
		
	}
		
	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <delta> <max_iterations>"
				+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>"
				+ " <numParameters> <avg-node-degree>";
	}

}
