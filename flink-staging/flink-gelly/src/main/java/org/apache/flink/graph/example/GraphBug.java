package org.apache.flink.graph.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

/**
 * Finds the components of the bipartite graph.
 */
@SuppressWarnings("serial")
public class GraphBug implements ProgramDescription {

	@Override
	public String getDescription() {
		return "Find the graph's components";
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: FindComponents <input-edge-list> <output-components>");
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Edge<Long, NullValue>> inputEdges = env.readCsvFile(args[0])
				.fieldDelimiter("\t").lineDelimiter("\n")
				.types(Long.class, Long.class)
				.map(new MapFunction<Tuple2<Long,Long>, Edge<Long, NullValue>>() {

					public Edge<Long, NullValue> map(Tuple2<Long, Long> tuple) {
						return new Edge<Long, NullValue>(tuple.f0, tuple.f1, NullValue.getInstance());
					}
				});

		// create the graph
		Graph<Long, Long, NullValue> mappedGraph = Graph.fromDataSet(inputEdges, 
				new MapFunction<Long, Long>() {
					public Long map(Long vertexId) {
						return vertexId;
					}
				}, env).getUndirected();
		
		DataSet<Vertex<Long, Long>> verticesWithComponents = mappedGraph.runVertexCentricIteration(
				new MinComponent(), new SendId(), 50).getVertices();
		
		verticesWithComponents.first(1).print();
		
		verticesWithComponents.writeAsCsv(args[1], "\n", "\t");

		env.execute();
	}

	public static final class MinComponent extends VertexUpdateFunction<Long, Long, Long> {

		public void updateVertex(Long vertexKey, Long vertexValue, MessageIterator<Long> inMessages) {

			long minId = Long.MAX_VALUE;
			for (Long msg : inMessages) {
				if (msg < minId) {
					minId = msg;
				}
			}
			if (minId < vertexValue) {
				setNewVertexValue(minId);
			}
		}
	}
	
	public static final class SendId extends MessagingFunction<Long, Long, Long, NullValue> {

		public void sendMessages(Long vertexKey, Long vertexValue) {
			sendMessageToAllNeighbors(vertexValue);
			
		}
	}
}