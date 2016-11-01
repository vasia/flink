package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

/**
 * Contains User Defined Functions for a bulk synchronous iteration on WindowedStreams on input and feedback:
 * - entry: takes input windows, may save initial local state for the iteration, usually puts out to the feedback
 * - step: takes feedback windows, may update local state, usually puts out to feedback (or to output for early results)
 * - onTermination: is triggered when an iteration terminates. Reads local state and puts it out to output stream.
 *
 *  Usual lifecycle of an iteration:
 *  - window from input is processed by entry function
 *  - output of entry goes to feedback (will be passed through feedbackBuilder and then windowed for BSP)
 *  - feedback window gets processed by step function
 *  - ... (more iterations: step -> feedbackBuilder -> Windowing for BSP)
 *  - StreamIterationTermination decides that the iteration is over -> onTermination gets called
 *  - on Termination reads local state and puts out final results
 *
 * @param <IN>	  The input data type (goes into entry function)
 * @param <F_IN>  The feedback input data type (goes into step function)
 * @param <OUT>   The type of the iteration output (likely produced by onTermination, but also entry&step can output)
 * @param <R> 	  The type of the feedback output (as produced by entry or step)
 * @param <KEY>   The key type of both input and feedback windows (should be keyed the same way anyways)
 * @param <W_IN>  The Window type if the input
 */
@Public
public interface WindowLoopFunction<IN,F_IN,OUT,R,KEY,W_IN extends Window> extends Function, Serializable {
	void entry(LoopContext<KEY> ctx, Iterable<IN> input, Collector<Either<R,OUT>> out) throws Exception;
	void step(LoopContext<KEY> ctx, Iterable<F_IN> input, Collector<Either<R,OUT>> out) throws Exception;
	void onTermination(List<Long> timeContext, long superstep, Collector<Either<R,OUT>> out) throws Exception;
}
