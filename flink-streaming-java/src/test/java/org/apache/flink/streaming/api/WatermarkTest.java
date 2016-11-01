package org.apache.flink.streaming.api;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class WatermarkTest {

	@Test
	public void addRemoveTimestampTest() {
		Watermark mark1 = new Watermark(12);
		List<Long> ts = new LinkedList<Long>();
		ts.add(new Long(12));
		ts.add(new Long(5));
		Watermark mark2 = new Watermark(ts, 6);

		assert mark1.getFullTimestamp().get(0) == 12;

		List<Long> ts2 = new LinkedList<>(ts);
		ts2.add(new Long(6));
		assert mark2.getFullTimestamp().equals(ts2);

		mark1.addNestedTimestamp(5);
		mark2.removeNestedTimestamp();
		assert mark1.equals(mark2);
	}
}
