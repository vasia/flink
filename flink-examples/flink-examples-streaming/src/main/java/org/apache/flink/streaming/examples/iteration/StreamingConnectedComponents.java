package org.apache.flink.streaming.examples.iteration;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.FeedbackBuilder;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamingConnectedComponents {
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	public static void main(String[] args) throws Exception {
		StreamingConnectedComponents example = new StreamingConnectedComponents();
		example.run();
	}


	/**
	 *
	 * @throws Exception
	 */
	public StreamingConnectedComponents() throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(4);

		DataStream<Tuple2<Long, List<Long>>> inputStream = env.addSource(new CCSampleSrc());
		WindowedStream<Tuple2<Long, List<Long>>, Long, TimeWindow> winStream =

			inputStream.keyBy(new KeySelector<Tuple2<Long, List<Long>>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, List<Long>> value) throws Exception {
					return value.f0;
				}
			}).timeWindow(Time.milliseconds(1000));
		
			winStream.iterateSyncFor(4,
				new MyWindowLoopFunction(),
				new MyFeedbackBuilder(),
				new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO))
			.print();
	}

	protected void run() throws Exception {
		System.err.println(env.getExecutionPlan());
		env.execute("Streaming Sync Iteration Example (CC)");
	}

	private static class MyFeedbackBuilder implements FeedbackBuilder<Tuple2<Long, Long>, Long> {
		@Override
		public KeyedStream<Tuple2<Long, Long>, Long> feedback(DataStream<Tuple2<Long, Long>> input) {
			return input.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, Long> value) throws Exception {
					return value.f0;
				}
			});
		}
	}

	private static final List<Tuple3<Long, List<Long>, Long>> sampleStream = Lists.newArrayList(

			// vertexId - List of neighbors - timestamp
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(2l, 3l), 1000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(1l), 1000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(1l), 1000l),
		new Tuple3<>(4l, (List<Long>) Lists.newArrayList(5l, 6l), 1000l),
		new Tuple3<>(5l, (List<Long>) Lists.newArrayList(4l), 1000l),
		new Tuple3<>(6l, (List<Long>) Lists.newArrayList(4l), 1000l),

		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(1l, 3l), 2000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(2l), 2000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(2l), 2000l),
		new Tuple3<>(4l, (List<Long>) Lists.newArrayList(5l, 6l), 2000l),
		new Tuple3<>(5l, (List<Long>) Lists.newArrayList(4l), 2000l),
		new Tuple3<>(6l, (List<Long>) Lists.newArrayList(4l), 2000l),
		new Tuple3<>(10l, (List<Long>) Lists.newArrayList(8l, 9l), 2000l),
		new Tuple3<>(9l, (List<Long>) Lists.newArrayList(10l), 2000l),
		new Tuple3<>(8l, (List<Long>) Lists.newArrayList(10l), 2000l),

		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(2l, 1l), 3000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(3l), 3000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(3l), 3000l),

		new Tuple3<>(4l, (List<Long>) Lists.newArrayList(1l), 4000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(4l), 4000l)

	);


	private static class CCSampleSrc extends RichSourceFunction<Tuple2<Long, List<Long>>> {

		@Override
		public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) throws Exception {
			long curTime = -1;
			for (Tuple3<Long, List<Long>, Long> next : sampleStream) {
				ctx.collectWithTimestamp(new Tuple2<>(next.f0, next.f1), next.f2);

				if (curTime == -1) {
					curTime = next.f2;
				}
				if (curTime < next.f2) {
					curTime = next.f2;
					ctx.emitWatermark(new Watermark(curTime - 1));

				}
			}
		}

		@Override
		public void cancel() {
		}
	}


	private static class MyWindowLoopFunction implements WindowLoopFunction<Tuple2<Long, List<Long>>, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>, Long, TimeWindow>, Serializable {
		// context -> (vertex -> neighbors)
		Map<List<Long>, Map<Long, List<Long>>> neighboursPerContext = new HashMap<>();
		Map<List<Long>, Map<Long, Long>> componentIdsPerContext = new HashMap<>();

		public List<Long> getNeighbours(List<Long> timeContext, Long nodeID) {
			return neighboursPerContext.get(timeContext).get(nodeID);
		}

		@Override
		public void entry(LoopContext<Long> ctx, Iterable<Tuple2<Long, List<Long>>> iterable, Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> collector) {
			Map<Long, List<Long>> adjacencyList = neighboursPerContext.get(ctx.getContext());
			if (adjacencyList == null) {
				adjacencyList = new HashMap<>();
				neighboursPerContext.put(ctx.getContext(), adjacencyList);
			}

			Map<Long, Long> components = componentIdsPerContext.get(ctx.getContext());
			if (components == null) {
				components = new HashMap<>();
				componentIdsPerContext.put(ctx.getContext(), components);
			}

			Tuple2<Long, List<Long>> next = iterable.iterator().next();
			adjacencyList.put(next.f0, next.f1);
			// save component id to local state
			components.put(next.f0, next.f0);

			// send compId into feedback loop
			collector.collect(new Either.Left(new Tuple2<>(ctx.getKey(),  ctx.getKey())));

			System.err.println("ENTRY (" + ctx.getKey() + "):: " + Arrays.toString(ctx.getContext().toArray()) + " -> " + adjacencyList);
		}

		@Override
		public void step(LoopContext<Long> ctx, Iterable<Tuple2<Long, Long>> iterable, Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> collector) {
			Map<Long, Long> minIds = componentIdsPerContext.get(ctx.getContext());
			for (Tuple2<Long, Long> entry : iterable) {
				Long current = minIds.get(entry.f0);
				if (current == null) {
					minIds.put(entry.f0, entry.f1);
				} else {
					minIds.put(entry.f0, Math.min(current, entry.f1));
				}
			}

			for (Map.Entry<Long, Long> entry : minIds.entrySet()) {
				List<Long> neighbourIDs = getNeighbours(ctx.getContext(), entry.getKey());
				Long currentId = entry.getValue();

				// update current component Id
				componentIdsPerContext.get(ctx.getContext()).put(entry.getKey(), currentId);

				// distributed new component Id
				for (Long neighbourID : neighbourIDs) {
					collector.collect(new Either.Left(new Tuple2<>(neighbourID, currentId)));
				}
			}
			System.err.println("POST-STEP:: " + Arrays.toString(ctx.getContext().toArray()) + " (" + ctx.getSuperstep() + ")" + componentIdsPerContext.get(ctx.getContext()));
		}

		@Override
		public void onTermination(List<Long> timeContext, long superstep, Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) {
			Map<Long, Long> vertexStates = componentIdsPerContext.get(timeContext);
			System.err.println("ON TERMINATION:: " + timeContext + "::" + vertexStates);
			if(vertexStates != null){
				for (Map.Entry<Long, Long> compId : componentIdsPerContext.get(timeContext).entrySet()) {
					out.collect(new Either.Right(new Tuple2(compId.getKey(), compId.getValue())));
				}	
			}
		}
	}
}
