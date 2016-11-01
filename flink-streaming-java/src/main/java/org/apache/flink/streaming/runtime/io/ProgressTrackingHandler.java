/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProgressTrackingHandler implements Serializable {
	private int numberOfInputChannels;

	private Map<List<Long>, Tuple2<Long, Boolean>>[] watermarks;
	private Map<List<Long>, Long> lastEmittedWatermarks = new HashMap<>();

	public ProgressTrackingHandler(int numberOfInputChannels) {
		this.numberOfInputChannels = numberOfInputChannels;
		watermarks = new HashMap[numberOfInputChannels];
		for (int i = 0; i < numberOfInputChannels; i++) {
			watermarks[i] = new HashMap<>();
		}
	}

	public Watermark getNextWatermark(Watermark watermark, int currentChannel) {
		Long timestamp = watermark.getTimestamp();
		List<Long> context = watermark.getContext();

		// Check if whole context is finished and clean up
		if (watermark.getTimestamp() == Long.MAX_VALUE) {
			watermarks[currentChannel].put(context, new Tuple2<>(Long.MAX_VALUE, false));
			for (int i = 0; i < numberOfInputChannels; i++) {
				Tuple2<Long, Boolean> entry = watermarks[i].get(context);
				if (entry == null || entry.f0 != Long.MAX_VALUE) {
					return null;
				}
			}
			watermarks[currentChannel].remove(context);
			lastEmittedWatermarks.remove(context);
			return null;
		}
		// Update local watermarks and eventually send out a new
		Long currentMax = watermarks[currentChannel].get(context) != null ?
			watermarks[currentChannel].get(context).f0 : null;
		// Only go on if the current timestamp is actually higher for this context
		if (currentMax == null || timestamp > currentMax) {
			watermarks[currentChannel].put(context, new Tuple2<>(timestamp, watermark.iterationDone()));

			// find out the minimum over all input channels for this context
			Long newMin = Long.MAX_VALUE;
			boolean isDone = true;
			//if(taskIndex == 0) {
			//	System.out.println(watermarks[0].get(context) + " - " +
			//		watermarks[1].get(context) + " - " +
			//		watermarks[2].get(context) + " - " +
			//		watermarks[3].get(context) + " - ");
			//}
			for (int i = 0; i < numberOfInputChannels; i++) {
				Long channelMax = watermarks[i].get(context) != null ? watermarks[i].get(context).f0 : null;
				if (channelMax == null) {
					return null;
				}

				if (!watermarks[i].get(context).f1) {
					isDone = false;
				}
				if (channelMax < newMin) {
					newMin = channelMax;
				}
			}

			if (isDone) {
				return new Watermark(context, Long.MAX_VALUE, true, watermark.iterationOnly());
			} else {
				// if the new minimum of all channels is larger than the last emitted watermark
				// put out a new one
				Long lastEmitted = lastEmittedWatermarks.get(context);
				if (lastEmitted == null || newMin > lastEmitted) {
					lastEmittedWatermarks.put(context, newMin);
					return new Watermark(context, newMin, false, watermark.iterationOnly());
				}
			}
		}
		return null;
	}

}
