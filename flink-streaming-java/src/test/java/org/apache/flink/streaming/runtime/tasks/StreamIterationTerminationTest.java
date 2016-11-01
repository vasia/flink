package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.progress.FixpointIterationTermination;
import org.apache.flink.streaming.runtime.tasks.progress.StreamIterationTermination;
import org.apache.flink.streaming.runtime.tasks.progress.StructuredIterationTermination;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class StreamIterationTerminationTest {

	@Test
	public void fixpointProgressTest() {
		StreamIterationTermination termination = new FixpointIterationTermination();

		List<Long> context0 = new LinkedList<>();
		context0.add(new Long(0));
		List<Long> context1 = new LinkedList<>();
		context1.add(new Long(1));

		Watermark watermark01 = new Watermark(context0, 1);
		Watermark watermark10 = new Watermark(context1, 0);

		//make sure both contexts exist
		termination.observeRecord(new StreamRecord<>("", context0, 4));
		assert !termination.terminate(context0);
		termination.observeRecord(new StreamRecord<>("", context1, 2));
		assert !termination.terminate(context1);

		//no termination yet since both contexts active
		termination.observeWatermark(watermark01);
		assert !termination.terminate(context0);
		termination.observeWatermark(watermark10);
		assert !termination.terminate(context1);

		// this time, only context 0 stays active
		termination.observeRecord(new StreamRecord<>("", context0, 4));
		assert !termination.terminate(context0);

		Watermark watermark02 = new Watermark(context0, 2);
		Watermark watermark12 = new Watermark(context1, 2);

		termination.observeWatermark(watermark02);
		assert !termination.terminate(context0);
		termination.observeWatermark(watermark12);
		assert termination.terminate(context1);
	}

	@Test
	public void structuredIterationProgressTest() {
		StructuredIterationTermination termination = new StructuredIterationTermination(2);

		List<Long> context0 = new LinkedList<>();
		context0.add(new Long(0));
		List<Long> context1 = new LinkedList<>();
		context1.add(new Long(1));

		Watermark watermark00 = new Watermark(context0, 0);
		Watermark watermark01 = new Watermark(context0, 1);
		Watermark watermark11 = new Watermark(context1, 1);

		termination.observeWatermark(watermark00);
		assert(termination.terminate(context0)) == false;
		termination.observeWatermark(watermark11);
		assert(termination.terminate(context1)) == true;
		termination.observeWatermark(watermark01);
		assert termination.terminate(context0) == true;
	}
}
