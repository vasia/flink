package org.apache.flink.runtime.progress.messages

import org.apache.flink.api.common.JobID
import java.lang.Long;
import java.util.List;

case class ProgressMetricsReport(jobId: JobID, operatorId: Integer, instanceId: Integer, context: List[Long], startTS: Long, localEndTS: Long, endTS: Long)
