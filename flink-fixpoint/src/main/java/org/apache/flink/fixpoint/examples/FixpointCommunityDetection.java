package org.apache.flink.fixpoint.examples;

import java.util.Iterator;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fixpoint.api.FixedPointIteration;
import org.apache.flink.fixpoint.api.StepFunction;
import org.apache.flink.util.Collector;

public class FixpointCommunityDetection implements ProgramDescription {
	
	private static final double delta = 0.5;

	@SuppressWarnings("serial")
	public static void main(String... args) throws Exception {
		
		if (args.length < 4) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max_iterations>"
					+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>");
			return;
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);
		
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
		
		int maxIterations = Integer.parseInt(args[3]);
	
		DataSet<Tuple2<Long, Tuple2<Long, Double>>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new ComputeCommunities(), maxIterations, args[4]));

		result.print();
		env.execute("Fixed Point Community Detection");
		
	}
	
	@SuppressWarnings("serial")
	public static final class ComputeCommunities extends StepFunction<Long, Tuple2<Long, Double>, Double> {

		@Override
		public DataSet<Tuple2<Long, Tuple2<Long, Double>>> updateState(
			DataSet<Tuple4<Long, Long, Tuple2<Long, Double>, Double>> inNeighbors, 
			DataSet<Tuple2<Long, Tuple2<Long, Double>>> state) {
			
			// <vertexID, neighborID, neighborLabel, neighborScore, edgeWeight>
			DataSet<Tuple5<Long, Long, Long, Double, Double>> flattenedNeighborWithLabel = inNeighbors.map(new FlattenNeighbors());
			// <vertexID, candidateLabel, labelScore>
			DataSet<Tuple3<Long, Long, Double>> candidateLabels = flattenedNeighborWithLabel.groupBy(0, 2).reduceGroup(new ScoreLabels());
			// <vertexID, newLabel>
			DataSet<Tuple2<Long, Long>> verticesWithNewLabels = candidateLabels.groupBy(0)
					.reduce(new ReduceFunction<Tuple3<Long, Long, Double>>() {
						
						@Override
						public Tuple3<Long, Long, Double> reduce(Tuple3<Long, Long, Double> value1,
								Tuple3<Long, Long, Double> value2) throws Exception {
							if (value1.f2 > value2.f2) {
								return value1;
							}
							else {
								return value2;
							}
						}
					}).project(0, 1).types(Long.class, Long.class); 
					
			DataSet<Tuple2<Long, Tuple2<Long, Double>>> verticesWithNewScoredLabels = 
					verticesWithNewLabels.join(flattenedNeighborWithLabel).where(0).equalTo(0)
					// <vertexID, newLabel, labelScore>
					.filter(new FilterFunction<Tuple2<Tuple2<Long,Long>,Tuple5<Long,Long,Long,Double,Double>>>() {
						public boolean filter(
								Tuple2<Tuple2<Long, Long>, Tuple5<Long, Long, Long, Double, Double>> value)
								throws Exception {
							return ((value.f0.f1).equals(value.f1.f2));
						}
					})
					.map(new MapFunction<Tuple2<Tuple2<Long,Long>,Tuple5<Long,Long,Long,Double,Double>>, 
							Tuple3<Long, Long, Double>>() {
						public Tuple3<Long, Long, Double> map(
								Tuple2<Tuple2<Long, Long>, Tuple5<Long, Long, Long, Double, Double>> value)
								throws Exception {
							// <vertexID, label, score>
							return new Tuple3<Long, Long, Double>(value.f0.f0, value.f0.f1, value.f1.f3);
						}
					})
					.groupBy(0).reduce(new ReduceFunction<Tuple3<Long, Long, Double>>() {
						// find max-score label
						@Override
						public Tuple3<Long, Long, Double> reduce(Tuple3<Long, Long, Double> value1,
								Tuple3<Long, Long, Double> value2) throws Exception {
							if (value1.f2 > value2.f2) {
								return value1;
							}
							else {
								return value2;
							}
						}
					}).join(state).where(0).equalTo(0).map(new NewScoreMapper());
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
	
	public static final class ScoreLabels implements GroupReduceFunction<Tuple5<Long, Long, Long, Double, Double>, 
		Tuple3<Long, Long, Double>> {

		private static final long serialVersionUID = 1L;
		private double scoreSum = 0.0; 
		private Tuple5<Long, Long, Long, Double, Double> current;
		private Tuple3<Long, Long, Double> result = new Tuple3<Long, Long, Double>();

		@Override
		public void reduce(
				Iterable<Tuple5<Long, Long, Long, Double, Double>> values,
				Collector<Tuple3<Long, Long, Double>> out) throws Exception {
			
			Iterator<Tuple5<Long, Long, Long, Double, Double>> valuesIt = values.iterator();
			Tuple5<Long, Long, Long, Double, Double> first = valuesIt.next();
			result.setField(first.f0, 0); // vertexID
			result.setField(first.f2, 1); // label
			scoreSum = first.f3 * first.f4; // score * edgeWeight
			
			while (valuesIt.hasNext()) {
				current = valuesIt.next();
				// alternatively also multiply with degree(vertex)^m
				scoreSum += current.f3 * current.f4;
			}
			result.setField(scoreSum, 2);
			out.collect(result);
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
	Tuple2<Long, Tuple2<Long, Double>>>, 
		Tuple2<Long, Tuple2<Long, Double>>>{
		
		private static final long serialVersionUID = 1L;
		private int superstep;
		
		@Override
		public void open(Configuration conf){
			superstep = getIterationRuntimeContext().getSuperstepNumber();
		}
		
		@Override
		public Tuple2<Long, Tuple2<Long, Double>> map(Tuple2<Tuple3<Long, Long, Double>, 
				Tuple2<Long, Tuple2<Long, Double>>> value)
				throws Exception {
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
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations> "
				+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>";
	}

}
