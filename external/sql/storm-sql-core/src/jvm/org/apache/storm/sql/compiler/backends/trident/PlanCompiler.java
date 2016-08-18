package org.apache.storm.sql.compiler.backends.trident;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.trident.AbstractTridentProcessor;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.IAggregatableStream;

import java.util.Map;

public class PlanCompiler {
    private Map<String, ISqlTridentDataSource> sources;
    private final JavaTypeFactory typeFactory;

    public PlanCompiler(Map<String, ISqlTridentDataSource> sources, JavaTypeFactory typeFactory) {
        this.sources = sources;
        this.typeFactory = typeFactory;
    }

    public AbstractTridentProcessor compileForTest(RelNode plan) throws Exception {
        final TridentTopology topology = new TridentTopology();

        TridentLogicalPlanCompiler compiler = new TridentLogicalPlanCompiler(sources, typeFactory, topology);
        final IAggregatableStream stream = compiler.traverse(plan);

        return new AbstractTridentProcessor() {
            @Override
            public Stream outputStream() {
                return stream.toStream();
            }

            @Override
            public TridentTopology build(Map<String, ISqlTridentDataSource> sources) {
                return topology;
            }
        };
    }

    public TridentTopology compile(RelNode plan) throws Exception {
        TridentTopology topology = new TridentTopology();

        TridentLogicalPlanCompiler compiler = new TridentLogicalPlanCompiler(sources, typeFactory, topology);
        compiler.traverse(plan);

        return topology;
    }




}
