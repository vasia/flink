package org.apache.flink.streaming.api.datastream;


import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.transformations.CoFeedbackTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowMultiPassOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.progress.StreamIterationTermination;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * 
 * IterativeWindowStream represents a windowed stream within the scope of an iteration.
 * IterativeWindowStreams can be typically used in order to express many types of iterative computation and consist
 * of an input WindowedStream, a CoWindowTerminateFunction which defines the final type of its output and 
 * 
 * 
 * @param <IN>
 * @param <IN_W>
 * @param <F>
 * @param <K>
 * @param <R>
 * @param <S>
 */
@Public
public class IterativeWindowStream<IN, IN_W extends Window, F, K, R, S> {
	private DataStream<S> outStream;
	public IterativeWindowStream(WindowedStream<IN, K, IN_W> input, WindowLoopFunction<IN, F, S, R, K, IN_W> coWinTerm, 
		StreamIterationTermination terminationStrategy, 
		FeedbackBuilder<R, K> feedbackBuilder, 
		TypeInformation<R> feedbackType, long waitTime) throws Exception {
		
		WindowedStream<IN, K, IN_W> windowedStream1 = input;
		
		// create feedback edge
		CoFeedbackTransformation<R> coFeedbackTransformation = new CoFeedbackTransformation<>(windowedStream1.getInput().getParallelism(),
			feedbackType, waitTime, windowedStream1.getInput().getTransformation().getScope(), terminationStrategy);

		// create feedback source
		KeyedStream<R, K> feedbackSourceStream = feedbackBuilder.feedback(new DataStream<>(windowedStream1.getExecutionEnvironment(), coFeedbackTransformation));
		WindowAssigner assigner = TumblingEventTimeWindows.of(Time.milliseconds(1));
		WindowedStream<F, K, TimeWindow> windowedStream2 = new WindowedStream<>(feedbackSourceStream, assigner);

		// create feedback sink
		Tuple2<DataStream<R>, DataStream<S>> streams = applyCoWinTerm(coWinTerm, windowedStream1, windowedStream2, feedbackSourceStream.getKeySelector());
		coFeedbackTransformation.addFeedbackEdge(streams.f0.getTransformation());
		outStream = streams.f1;
	}

	public Tuple2<DataStream<R>, DataStream<S>> applyCoWinTerm(WindowLoopFunction coWinTerm,
															   WindowedStream<IN, K, IN_W> windowedStream1,
															   WindowedStream<F, K, TimeWindow> windowedStream2, 
															   KeySelector<R, K> feedbackKeySelector) throws Exception {

		TypeInformation<S> outTypeInfo = TypeExtractor.createTypeInfo(coWinTerm,
			WindowLoopFunction.class, coWinTerm.getClass(), 2);
		TypeInformation<R> intermediateFeedbackTypeInfo = TypeExtractor.createTypeInfo(WindowLoopFunction.class,
			coWinTerm.getClass(), 1, windowedStream1.getInputType(), windowedStream2.getInputType());
		
		TwoInputTransformation<IN, F, Either<R, S>> transformation = getTransformation(
			coWinTerm,
			windowedStream1,
			windowedStream2,
			feedbackKeySelector,
			outTypeInfo,
			intermediateFeedbackTypeInfo);
		// TODO check if this is necessary
		transformation.setStateKeySelectors(windowedStream1.getInput().getKeySelector(), windowedStream2.getInput().getKeySelector());
		transformation.setStateKeyType(windowedStream1.getInput().getKeyType());

		DataStream<Either<R, S>> combinedOutputStream = new SingleOutputStreamOperator<Either<R, S>>(windowedStream1.getExecutionEnvironment(), transformation);

		SplitStream<Either<R, S>> splitStream = combinedOutputStream.split(new OutputSelector<Either<R, S>>() {
			@Override
			public Iterable<String> select(Either<R, S> value) {
				return value.isLeft() ? Collections.singletonList("FEEDBACK") : Collections.singletonList("FORWARD");
			}
		});
		DataStream<R> feedbackStream = splitStream.select("FEEDBACK").map(new MapFunction<Either<R, S>, R>() {
			@Override
			public R map(Either<R, S> value) throws Exception {
				return value.left();
			}
		});
		DataStream<S> forwardStream = splitStream.select("FORWARD").map(new MapFunction<Either<R, S>, S>() {
			@Override
			public S map(Either<R, S> value) throws Exception {
				return value.right();
			}
		}); //

		feedbackStream.getTransformation().setParallelism(transformation.getParallelism());
		feedbackStream.getTransformation().setOutputType(intermediateFeedbackTypeInfo);
		forwardStream.getTransformation().setParallelism(transformation.getParallelism());
		forwardStream.getTransformation().setOutputType(outTypeInfo);

		return new Tuple2(feedbackStream, forwardStream);
	}

	public DataStream<S> loop() throws Exception {
		return outStream;
	}

	public TwoInputTransformation<IN, F, Either<R, S>> getTransformation(
		final WindowLoopFunction<IN, F, S, R, K, IN_W> coWinTerm,
		WindowedStream<IN, K, IN_W> windowedStream1,
		WindowedStream<F, K, TimeWindow> windowedStream2,
		KeySelector<R, K> feedbackSelector, 
		TypeInformation<S> outTypeInfo,
		TypeInformation<R> intermediateFeedbackTypeInfo) throws Exception {

		TypeInformation<Either<R, S>> eitherTypeInfo = new EitherTypeInfo<>(intermediateFeedbackTypeInfo, outTypeInfo);
		
		Tuple2<String, WindowOperator> stepDiscretizer =
			getWindowOperator(windowedStream2, new WrappedWindowFunction2<F, Either<R, S>, K, TimeWindow>(coWinTerm), eitherTypeInfo);

		String opName = "WindowMultiPass(" + stepDiscretizer.f0 + ")"; 
		WindowMultiPassOperator combinedOperator = new WindowMultiPassOperator(windowedStream1.getInput().getKeySelector(),feedbackSelector, stepDiscretizer.f1, coWinTerm);
		return new TwoInputTransformation<>(
			windowedStream1.getInput().getTransformation(),
			windowedStream2.getInput().getTransformation(),
			opName,
			combinedOperator,
			eitherTypeInfo,
			windowedStream1.getInput().getParallelism()
		);
	}

	// rougly the same like WindowedStream#apply(Windowfunction, resultType) but for two window inputs
	public <T, WIN extends Window> Tuple2<String, WindowOperator>
	getWindowOperator(WindowedStream<T, K, WIN> windowedStream, WindowFunction<T, Either<R, S>, K, WIN> function, TypeInformation<Either<R, S>> resultType) {
		KeyedStream<T, K> input = windowedStream.getInput();
		WindowAssigner windowAssigner = windowedStream.getWindowAssigner();
		StreamExecutionEnvironment environment = windowedStream.getExecutionEnvironment();

		String callLocation = Utils.getCallLocationName();
		String udfName = "WindowedStream." + callLocation;

		String opName;
		KeySelector<T, K> keySel = input.getKeySelector();

		WindowOperator<K, T, Iterable<T>, Either<R, S>, WIN> operator;

		if (windowedStream.getEvictor() != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(environment.getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + windowedStream.getTrigger() + ", " + windowedStream.getEvictor() + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(environment.getConfig()),
					keySel,
					input.getKeyType().createSerializer(environment.getConfig()),
					stateDesc,
					new InternalIterableWindowFunction<>(function),
					windowedStream.getTrigger(),
					windowedStream.getEvictor(),
					windowedStream.getAllowedLateness(),
					null);

		} else {
			ListStateDescriptor<T> stateDesc = new ListStateDescriptor<>("window-contents",
				input.getType().createSerializer(environment.getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + windowedStream.getTrigger() + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(environment.getConfig()),
					keySel,
					input.getKeyType().createSerializer(environment.getConfig()),
					stateDesc,
					new InternalIterableWindowFunction<>(function),
					windowedStream.getTrigger(),
					windowedStream.getAllowedLateness(),
					null);
		}

		return new Tuple2(opName, operator);
	}
	

	private static class WrappedWindowFunction2<IN, OUT, K, W extends TimeWindow> extends RichWindowFunction<IN,OUT,K,W> {

		WindowLoopFunction coWinTerm;

		public WrappedWindowFunction2(WindowLoopFunction coWinTerm) {
			this.coWinTerm = coWinTerm;
		}

		public void apply(K key, W window, Iterable<IN> input, Collector<OUT> out) throws Exception {
			coWinTerm.step(new LoopContext(window.getTimeContext(), window.getEnd(), key), input, out);
		}
	}
}
