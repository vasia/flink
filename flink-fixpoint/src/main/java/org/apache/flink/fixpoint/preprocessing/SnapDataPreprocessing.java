package org.apache.flink.fixpoint.preprocessing;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SnapDataPreprocessing implements ProgramDescription {
	
	public static void main(String... args) throws Exception {
			
		if (args.length < 3) {
			System.err.println("Parameters: <input-file> <output-file-vertices> <output-file-edges>");
			return;
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();	

		// ignore lines starting with '#' and convert the lines into 2-tuples
		DataSet<Tuple2<Long, Long>> edges = env.readTextFile(args[0])
				.flatMap(new IgnoreHashLinesMapper());
		
		// generate all vertex IDs from the edges
		DataSet<Tuple1<Long>> vertices = edges.flatMap(new FlatMapFunction<Tuple2<Long, Long>, Tuple1<Long>>() {
			private static final long serialVersionUID = 1L;
			public void flatMap(Tuple2<Long, Long> value, Collector<Tuple1<Long>> out) throws Exception {
				out.collect(new Tuple1<Long>(value.f0));
				out.collect(new Tuple1<Long>(value.f1));
			}
		}).distinct();		
		
		DataSet<Tuple2<Long, Long>> edgesWithSelfEdges = edges.union(vertices
				.map(new MapFunction<Tuple1<Long>, Tuple2<Long, Long>>() {
					private static final long serialVersionUID = 1L;
					public Tuple2<Long, Long> map(Tuple1<Long> value) throws Exception {
						return new Tuple2<Long, Long>(value.f0, value.f0);
					}			
		}));
		
		vertices.writeAsCsv(args[1], "\n", "\t");
		edgesWithSelfEdges.writeAsCsv(args[2], "\n", "\t");
		
		env.execute("SNAP Preprocessing");
			
	}

	@Override
	public String getDescription() {
		return "Parameters: <input-file> <output-file-vertices> <output-file-edges>";
	}
	
	public static final class IgnoreHashLinesMapper implements FlatMapFunction<String, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<Long, Long>> out)
				throws Exception {
			if (!(value.startsWith("#"))) {
				String[] tokens = value.split("\\s+"); 
				out.collect(new Tuple2<Long, Long>(Long.parseLong(tokens[0]), Long.parseLong(tokens[1])));
			}
			
		}
		
	}

}
