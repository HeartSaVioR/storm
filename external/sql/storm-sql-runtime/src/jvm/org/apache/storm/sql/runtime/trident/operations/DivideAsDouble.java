package org.apache.storm.sql.runtime.trident.operations;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class DivideAsDouble extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Number n1 = (Number)tuple.get(0);
        Number n2 = (Number)tuple.get(1);
        collector.emit(new Values(n1.doubleValue() / n2.doubleValue()));
    }
}