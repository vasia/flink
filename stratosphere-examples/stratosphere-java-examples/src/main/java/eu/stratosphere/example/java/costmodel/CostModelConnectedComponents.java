package eu.stratosphere.example.java.costmodel;

import java.util.Iterator;

import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.aggregators.LongSumAggregator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;

/**
 * A Connected Components implementation to test the cost model functionality.
 * The algorithm starts running Bulk iterations until the cost of the dependency
 * plan becomes less than the cost of the bulk plan or until the maximum number of iterations.
 * In the case of cost-based convergence of the bulk plan, the execution continues
 * with a dependency plan, for the remaining iterations or until no element changes.
 *
 */
@SuppressWarnings("serial")
public class CostModelConnectedComponents implements ProgramDescription {
	
	private static final String UPDATED_ELEMENTS_AGGR = "updated.elements.aggr";

	public static void main(String... args) throws Exception {
		if (args.length < 6) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>" +
					"<number-of-vertices> <avg-node-degree>");
			return;
		}
		
		final int maxIterations = Integer.parseInt(args[3]);
		
		final int numVertices = Integer.parseInt(args[4]);
		
		final double avgDegree = Double.parseDouble(args[5]);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		env.setDegreeOfParallelism(1);
		
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = env.readCsvFile(args[0]).types(Long.class).map(new DuplicateValue());
		
		DataSet<Tuple2<Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class);
		
		DataSet<Tuple2<Long, Long>> bulkResult = doBulkCostConnectedComponents(verticesWithInitialId, edges, maxIterations,
				numVertices, avgDegree);		
		
		DataSet<Tuple2<Long, Long>> depResult = doDepCostConnectedComponents(bulkResult, edges, maxIterations);
		
		depResult.writeAsCsv(args[2] + "_dep", "\n", " ");
		
		env.execute("Cost CC (no mapper)");
		
	}
	
	public static DataSet<Tuple2<Long, Long>> doBulkCostConnectedComponents(DataSet<Tuple2<Long, Long>> verticesWithInitialId, 
			DataSet<Tuple2<Long, Long>> edges, int maxIterations, int numVertices, double avgDegree) {
		
		/**
		 *  === start bulk iterations === 
		 */
		
		// open a bulk iteration
		IterativeDataSet<Tuple2<Long, Long>> iteration = verticesWithInitialId.iterate(maxIterations);
		
		// apply the step logic: join with the edges, select the minimum neighbor, update the component if the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
														.groupBy(0).aggregate(Aggregations.MIN, 1)
														.join(iteration).where(0).equalTo(0)
														.flatMap(new ComponentIdFilter());
		
		/** 
		 * Register updated elements aggregator and cost model convergence criterion.
		 * There is no need to also provide a convergence criterion for the case when no element changes, 
		 * before the cost of the dependency plan becomes less than the one of the bulk plan. 
		 * This case is covered because then the left-hand side of the cost model will then become 0.
		 */
		iteration.registerAggregationConvergenceCriterion(UPDATED_ELEMENTS_AGGR, new LongSumAggregator(), 
				new UpdatedElementsCostModelConvergence(numVertices, avgDegree));

		// close the bulk iteration
		DataSet<Tuple2<Long, Long>> bulkResult = iteration.closeWith(changes);
		
		return bulkResult;
		
	}
				
		
	public static DataSet<Tuple2<Long, Long>> doDepCostConnectedComponents(DataSet<Tuple2<Long, Long>> vertices, 
			DataSet<Tuple2<Long, Long>> edges, int maxIterations) {
		
		/**
		 *  === start dependency iterations === 
		 */
		
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> depIteration = 
				vertices.iterateDelta(vertices, maxIterations, 0);
				
		DataSet<Tuple1<Long>> candidates = depIteration.getWorkset().join(edges).where(0).equalTo(0)
				.projectSecond(1).types(Long.class);
		
		DataSet<Tuple1<Long>> grouped = candidates.groupBy(0).reduceGroup(new RemoveDuplicatesReduce());
		
		DataSet<Tuple2<Long, Long>> candidatesDependencies = 
				grouped.join(edges).where(0).equalTo(1).projectSecond(0, 1).types(Long.class, Long.class);
		
		DataSet<Tuple2<Long, Long>> verticesWithNewComponents = 
				candidatesDependencies.join(depIteration.getSolutionSet()).where(0).equalTo(0)
				.with(new NeighborWithComponentIDJoinDep())
				.groupBy(0).aggregate(Aggregations.MIN, 1);
		
		DataSet<Tuple2<Long, Long>> updatedComponentId = 
				verticesWithNewComponents.join(depIteration.getSolutionSet()).where(0).equalTo(0)
				.flatMap(new MinimumIdFilter());
		
		DataSet<Tuple2<Long, Long>> depResult = depIteration.closeWith(updatedComponentId, updatedComponentId);
		
		return depResult;
		
	}
	
	/* == Bulk iteration classes == */
	
	/**
	 * Function that turns a value into a 2-tuple where both fields are that value.
	 */
	@ConstantFields("0")
	public static final class DuplicateValue extends MapFunction<Tuple1<Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple1<Long> value) throws Exception {
			return new Tuple2<Long, Long>(value.f0, value.f0);
		}
	}
	
	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that
	 * a vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	public static final class NeighborWithComponentIDJoin extends JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	}
	
	/**
	 * The input is nested tuples ( (vertex-id, candidate-component) , (vertex-id, current-component) )
	 */
	public static final class ComponentIdFilter extends FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		private LongSumAggregator updatedElementsAggr;
		
		@Override
		public void open(Configuration conf) {
			updatedElementsAggr = getIterationRuntimeContext().getIterationAggregator(UPDATED_ELEMENTS_AGGR);
			int superstep = getIterationRuntimeContext().getSuperstepNumber();
			System.out.println("Bulk Iteration " + superstep);
		}
		
		@Override
		public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> value, Collector<Tuple2<Long, Long>> out) {
			if (value.f0.f1 < value.f1.f1) {
				out.collect(value.f0);
				updatedElementsAggr.aggregate(1);
			}
			else {
				out.collect(value.f1);
			}
		}
	}
	
	/* == Dependency iteration classes == */

	@ConstantFields("0")
	public static final class RemoveDuplicatesReduce extends GroupReduceFunction<Tuple1<Long>, Tuple1<Long>> {
		
		@Override
		public void reduce(Iterator<Tuple1<Long>> values,
				Collector<Tuple1<Long>> out) throws Exception {
			out.collect(values.next());
			
		}
	}
	
	public static final class NeighborWithComponentIDJoinDep extends JoinFunction
		<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		
		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> edge,
				Tuple2<Long, Long> vertexWithCompId) throws Exception {
			vertexWithCompId.setField(edge.f1, 0);
			return vertexWithCompId;
		}
}
	
	public static final class MinimumIdFilter extends FlatMapFunction
		<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {
	
		@Override
		public void open(Configuration conf) {
			System.out.println("Dependency Iteration " + getIterationRuntimeContext().getSuperstepNumber());
		}
		
		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> vertexWithNewAndOldId,
				Collector<Tuple2<Long, Long>> out) throws Exception {
						
			if ( vertexWithNewAndOldId.f0.f1 < vertexWithNewAndOldId.f1.f1 ) {
				out.collect(vertexWithNewAndOldId.f0);
			}
		}
	}


	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations> "
				+ "<number-of-vertices> <avg-node-degree>";
	}

}