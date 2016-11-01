package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.Window;

import static java.util.Objects.requireNonNull;

/**
 * ConnectedStreams represent two connected streams of (possibly) different data types.
 * Connected streams are useful for cases where operations on one stream directly
 * affect the operations on the other stream, usually via shared state between the streams.
 *
 * <p>An example for the use of connected streams would be to apply rules that change over time
 * onto another stream. One of the connected streams has the rules, the other stream the
 * elements to apply the rules to. The operation on the connected stream maintains the
 * current set of rules in the state. It may receive either a rule update and update the state
 * or a data element and apply the rules in the state to the element.
 *
 * <p>The connected stream can be conceptually viewed as a union stream of an Either type, that
 * holds either the first stream's type or the second stream's type.
 *
 * @param <IN1> Type of the first input data steam.
 * @param <IN2> Type of the second input data stream.
 */
@Public
public class ConnectedWindowedStreams<IN1, IN2, KEY, W1 extends Window, W2 extends Window> {

	protected final StreamExecutionEnvironment environment;
	protected final WindowedStream<IN1,KEY,W1> inputStream1;
	protected final WindowedStream<IN2,KEY,W2> inputStream2;

	protected ConnectedWindowedStreams(StreamExecutionEnvironment env, WindowedStream<IN1,KEY,W1> input1, WindowedStream<IN2,KEY,W2> input2) {
		this.environment = requireNonNull(env);
		this.inputStream1 = requireNonNull(input1);
		this.inputStream2 = requireNonNull(input2);
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return environment;
	}

	/**
	 * Returns the first {@link WindowedStream}.
	 *
	 * @return The first DataStream.
	 */
	public WindowedStream<IN1,KEY,W1> getFirstInput() {
		return inputStream1;
	}

	/**
	 * Returns the second {@link WindowedStream}.
	 *
	 * @return The second DataStream.
	 */
	public WindowedStream<IN2,KEY,W2> getSecondInput() {
		return inputStream2;
	}

	/**
	 * Gets the type of the first input
	 *
	 * @return The type of the first input
	 */
	public TypeInformation<IN1> getType1() {
		return inputStream1.getInput().getType();
	}

	/**
	 * Gets the type of the second input
	 *
	 * @return The type of the second input
	 */
	public TypeInformation<IN2> getType2() {
		return inputStream2.getInput().getType();
	}

	public ConnectedStreams<IN1,IN2> reduce(ReduceFunction<IN1> reduceFun1, ReduceFunction<IN2> reduceFun2) {
		return new ConnectedStreams<>(this.environment,
			this.inputStream1.reduce(reduceFun1),
			this.inputStream2.reduce(reduceFun2));
	}

	public <R1,R2> ConnectedStreams<R1,R2> fold(R1 initialVal1, FoldFunction<IN1,R1> foldFun1, R2 initialVal2, FoldFunction<IN2,R2> foldFun2) {
		return new ConnectedStreams<>(this.environment,
			this.inputStream1.fold(initialVal1, foldFun1),
			this.inputStream2.fold(initialVal2, foldFun2));
	}
}
