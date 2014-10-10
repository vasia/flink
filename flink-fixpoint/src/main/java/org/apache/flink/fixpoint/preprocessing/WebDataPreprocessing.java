package org.apache.flink.fixpoint.preprocessing;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class WebDataPreprocessing implements ProgramDescription {
	
	public static void main(String... args) throws Exception {
			
		if (args.length < 2) {
			System.err.println("Parameters: <input-file-edges> <output-file-edges>");
			return;
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();	

		/** for each edge, calculate the number of outgoing links
		/ and add it as a tuple field in the end **/
		DataSet<Tuple2<Long, Long>> edges = env.readCsvFile(args[0]).fieldDelimiter('\t').types(Long.class, Long.class);
		
		DataSet<Tuple2<Long, Long>> counts = edges.map(new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<Long, Long> map(Tuple2<Long, Long> value)
					throws Exception {
				return new Tuple2<Long, Long>(value.f0, 1L);
			}
			
		}).groupBy(0).sum(1).project(0, 1).types(Long.class, Long.class);
		
		DataSet<Tuple3<Long, Long, Long>> edgesWithOutlinks = edges.join(counts).where(0).equalTo(0)
				.flatMap(new FlatMapFunction<Tuple2<Tuple2<Long,Long>,Tuple2<Long,Long>>, Tuple3<Long, Long, Long>>() {
					private static final long serialVersionUID = 1L;

					public void flatMap(
							Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> value,
							Collector<Tuple3<Long, Long, Long>> out)
							throws Exception {
						out.collect(new Tuple3<Long, Long, Long> (value.f0.f0, value.f0.f1, value.f1.f1));
						
					}
				});
		
		edgesWithOutlinks.writeAsCsv(args[1], "\n", "\t");
		
		env.execute("Web Data Preprocessing");
			
	}

	@Override
	public String getDescription() {
		return "Parameters: <input-file-edges> <output-file-edges>";
	}
}
