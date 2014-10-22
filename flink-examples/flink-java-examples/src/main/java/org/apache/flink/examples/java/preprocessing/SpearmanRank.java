package org.apache.flink.examples.java.preprocessing;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * 
 * This program computes spearman's rank correlation for the ranks
 * on an original and the metric backbone graph, as computed by PageRank. 
 *
 */
public class SpearmanRank {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 6) {
			System.err.println("Usage: SpearmanRank <input-semimetric> <input-no-triangles> "
					+ "<output-semimetric-ranked> <output-no-triangles-ranked> <output-spearman-coefficient>"
					+ "<number_of_vertices>" );
			System.exit(-1);
		}
		
		final long numberOfVertices = Long.parseLong(args[5]);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// read the ranked semimetric input
		DataSet<Tuple2<Long, Double>> semimetricWithRanks = env.readCsvFile(args[0])
				.fieldDelimiter('\t')
				.types(Long.class, Double.class);
		
		// read the ranked non-semimetric input
		DataSet<Tuple2<Long, Double>> noTrianglesWithRanks = env.readCsvFile(args[1])
				.fieldDelimiter('\t')
				.types(Long.class, Double.class);
		
		// convert page ranks to spearman ranks
		DataSet<Tuple3<Long, Double, Long>> semimetricWithSpearmanRank = 
				semimetricWithRanks.map(new MapFunction<Tuple2<Long,Double>, Tuple3<Long, Double, Long>>() {

					public Tuple3<Long, Double, Long> map(
							Tuple2<Long, Double> value) {
						return new Tuple3<Long, Double, Long>(value.f0, value.f1, 1L);
					}
				}).groupBy(2).sortGroup(1, Order.DESCENDING).reduceGroup(
						new GroupReduceFunction<Tuple3<Long,Double,Long>, Tuple3<Long,Double,Long>>() {
							private static final long serialVersionUID = 1L;

							public void reduce(
									Iterable<Tuple3<Long, Double, Long>> values,
									Collector<Tuple3<Long, Double, Long>> out)
									throws Exception {
								
								long i = 1L;
								
								for (Tuple3<Long, Double, Long> v : values) {
									if(i < (numberOfVertices + 1)) {
										out.collect(new Tuple3<Long, Double, Long>(v.f0, v.f1, i));
										i++;
									}
								}
								
							}
				});
		
		// re-rank duplicates, by averaging what their ranks would be
		DataSet<Tuple2<Double, Double>> semimetricRanksWithMeans =  semimetricWithSpearmanRank.groupBy(1)
				.reduceGroup(new GroupReduceFunction<Tuple3<Long,Double,Long>, Tuple2<Double, Double>>() {
					public void reduce(
							Iterable<Tuple3<Long, Double, Long>> values,
							Collector<Tuple2<Double, Double>> out) {
						
						Iterator<Tuple3<Long, Double, Long>> iterator = values.iterator();
						
						if (iterator.hasNext()) {
							Tuple3<Long, Double, Long> firstTuple = iterator.next();
							double groupKey = firstTuple.f1;
							int groupSize = 1;
							long sum = firstTuple.f2;
							
							while (iterator.hasNext()) {
								Tuple3<Long, Double, Long> value  = iterator.next();
								groupSize++;
								sum += value.f2;
							}
							out.collect(new Tuple2<Double, Double>(groupKey, ((double) sum / (double) groupSize)));
						}
					}
				});
		
		DataSet<Tuple3<Long, Double, Double>> semimetricWithMeanRank = semimetricWithSpearmanRank
				.join(semimetricRanksWithMeans).where(1).equalTo(0).with(
						new FlatJoinFunction<Tuple3<Long,Double,Long>, Tuple2<Double,Double>, Tuple3<Long,Double,Double>>() {
							public void join(Tuple3<Long, Double, Long> first,
									Tuple2<Double, Double> second,
									Collector<Tuple3<Long, Double, Double>> out) {
								out.collect(new Tuple3<Long, Double, Double> (first.f0, first.f1, second.f1));
								
							}
				});
		
		// do the same for the non-semimetric dataset
		DataSet<Tuple3<Long, Double, Long>> noTrianglesWithSpearmanRank = 
				noTrianglesWithRanks.map(new MapFunction<Tuple2<Long,Double>, Tuple3<Long, Double, Long>>() {

					public Tuple3<Long, Double, Long> map(
							Tuple2<Long, Double> value) {
						return new Tuple3<Long, Double, Long>(value.f0, value.f1, 1L);
					}
				}).groupBy(2).sortGroup(1, Order.DESCENDING).reduceGroup(
						new GroupReduceFunction<Tuple3<Long,Double,Long>, Tuple3<Long,Double,Long>>() {
							private static final long serialVersionUID = 1L;

							public void reduce(
									Iterable<Tuple3<Long, Double, Long>> values,
									Collector<Tuple3<Long, Double, Long>> out)
									throws Exception {
								
								long i = 1L;
								
								for (Tuple3<Long, Double, Long> v : values) {
									if(i < (numberOfVertices + 1)) {
										out.collect(new Tuple3<Long, Double, Long>(v.f0, v.f1, i));
										i++;
									}
								}
								
							}
				});
		
		// re-rank duplicates, by averaging what their ranks would be
		DataSet<Tuple2<Double, Double>> noTrianglesRanksWithMeans =  noTrianglesWithSpearmanRank.groupBy(1)
				.reduceGroup(new GroupReduceFunction<Tuple3<Long,Double,Long>, Tuple2<Double, Double>>() {
					public void reduce(
							Iterable<Tuple3<Long, Double, Long>> values,
							Collector<Tuple2<Double, Double>> out) {
						
						Iterator<Tuple3<Long, Double, Long>> iterator = values.iterator();
						
						if (iterator.hasNext()) {
							Tuple3<Long, Double, Long> firstTuple = iterator.next();
							double groupKey = firstTuple.f1;
							int groupSize = 1;
							long sum = firstTuple.f2;
							
							while (iterator.hasNext()) {
								Tuple3<Long, Double, Long> value  = iterator.next();
								groupSize++;
								sum += value.f2;
							}
							out.collect(new Tuple2<Double, Double>(groupKey, ((double) sum / (double) groupSize)));
						}
					}
				});
				
				DataSet<Tuple3<Long, Double, Double>> noTrianglesWithMeanRank = noTrianglesWithSpearmanRank
						.join(noTrianglesRanksWithMeans).where(1).equalTo(0).with(
								new FlatJoinFunction<Tuple3<Long,Double,Long>, Tuple2<Double,Double>, Tuple3<Long,Double,Double>>() {
									public void join(Tuple3<Long, Double, Long> first,
											Tuple2<Double, Double> second,
											Collector<Tuple3<Long, Double, Double>> out) {
										out.collect(new Tuple3<Long, Double, Double> (first.f0, first.f1, second.f1));
										
									}
						});
		
		
		
		// store semimetric with Spearman ranks
		semimetricWithSpearmanRank.writeAsCsv(args[2], "\n", "\t");
		
		// store non-semimetric with Spearman ranks
		noTrianglesWithSpearmanRank.writeAsCsv(args[3], "\n", "\t");
		
		
		// count how many common ids exist in both datasets
		DataSet<Tuple1<Long>> commonIds = semimetricWithSpearmanRank.join(noTrianglesWithSpearmanRank)
				.where(0).equalTo(0).with(new FlatJoinFunction<Tuple3<Long,Double,Long>, Tuple3<Long,Double,Long>, 
						Tuple1<Long>>() {
							public void join(Tuple3<Long, Double, Long> first, Tuple3<Long, Double, Long> second,
									Collector<Tuple1<Long>> out) {
								out.collect(new Tuple1<Long>(1L));
							}
				}).groupBy(0).sum(0);
		
		commonIds.print();
		
		// compute the Spearman correlation coefficient
		DataSet<Tuple1<Double>> spearman  = semimetricWithMeanRank.join(noTrianglesWithMeanRank)
				.where(0).equalTo(0).with(new FlatJoinFunction<Tuple3<Long,Double,Double>, 
						Tuple3<Long,Double,Double>, Tuple2<Integer, Double>>() {
							public void join(Tuple3<Long, Double, Double> first,
									Tuple3<Long, Double, Double> second,
									Collector<Tuple2<Integer, Double>> out) {
								out.collect(new Tuple2<Integer, Double>(1, (first.f2 - second.f2)*(first.f2 - second.f2)));
								
							}		
				}).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer,Double>, Tuple1<Double>>() {
					public void reduce(Iterable<Tuple2<Integer, Double>> values,
							Collector<Tuple1<Double>> out) {
						
						double sum = 0.0;
						double r = 0.0;
						for (Tuple2<Integer, Double> value : values) {
							sum += value.f1;
						}
						r = (double) (1 - (6 * sum) / (numberOfVertices*((numberOfVertices*numberOfVertices) - 1)));
						out.collect(new Tuple1<Double>(r));
					}
					
				});

		// write spearman coefficient
		spearman.writeAsCsv(args[4]);
		
		env.execute();
	}
	
}
