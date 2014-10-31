package org.apache.flink.examples.java.preprocessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class CountMetric {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: CountMetric <input-file-path> <output-file-path>");
			System.exit(-1);
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);
		
		DataSet<Tuple4<Long,Long,Double,String>> labeledEdges = env.readCsvFile(args[0]).fieldDelimiter('\t')
				.types(Long.class, Long.class, Double.class, String.class);
		
		DataSet<Tuple1<Long>> metricCnt = labeledEdges.groupBy(3)
				.reduceGroup(new GroupReduceFunction<Tuple4<Long,Long,Double,String>, 
						Tuple1<Long>>() {
							public void reduce(Iterable<Tuple4<Long, Long, Double, String>> values,
									Collector<Tuple1<Long>> out) {
								long sum = 0;
								for (Tuple4<Long, Long, Double, String> value : values) {
									if (Boolean.parseBoolean(value.f3)) {
										sum++;
									}
								}
								out.collect(new Tuple1<Long>(sum));
							}
				});
		
		// <src, trg, weight>
		DataSet<Tuple3<Long, Long, Double>> metricOnly =  labeledEdges.flatMap(
						new FlatMapFunction<Tuple4<Long,Long,Double,String>, Tuple3<Long,Long, Double>>() {
							public void flatMap(
									Tuple4<Long,Long,Double,String> value,
									Collector<Tuple3<Long, Long, Double>> out) {
								if ((value.f3).equalsIgnoreCase("true")) {
									out.collect(new Tuple3<Long, Long, Double>(value.f0, value.f1, value.f2));
								}
							}
				});
		
		metricOnly.first(1000).writeAsCsv(args[1], "\n", "\t");
		// <srcId, count>
//		DataSet<Tuple2<Long, Long>> groupedBySrc = metricOnly.map(new MapFunction<
//				Tuple2<Long, Long>, Tuple3<Long, Long, Long>>() {
//					public Tuple3<Long, Long, Long> map(
//							Tuple2<Long, Long> value) throws Exception {
//						return new Tuple3<Long, Long, Long>(value.f0, value.f1, 1L);
//					}
//		}).groupBy(0).aggregate(Aggregations.SUM, 2).project(0, 2).types(Long.class, Long.class)
//		.map(new MapFunction<Tuple2<Long,Long>, Tuple3<Long, Long, Integer>>() {
//
//			public Tuple3<Long, Long, Integer> map(Tuple2<Long, Long> value) {
//				return new Tuple3<Long, Long, Integer> (value.f0, value.f1, -1);
//			}
//		}).groupBy(2).sortGroup(1, Order.DESCENDING).first(50).project(0, 1).types(Long.class, Long.class);
//		
//		metricCnt.writeAsCsv(args[1], "\n", "\t");
		
//		groupedBySrc.writeAsCsv(args[2], "\n", "\t");
		
		env.execute();
	}
	
}
