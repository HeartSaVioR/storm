package org.apache.storm.sql.runtime.trident.operations;

import clojure.lang.Numbers;
import org.apache.storm.sql.runtime.trident.TridentUtils;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class MaxBy implements CombinerAggregator<Number> {

    private final String inputFieldName;

    public MaxBy(String inputFieldName) {
        this.inputFieldName = inputFieldName;
    }

    @Override
    public Number init(TridentTuple tuple) {
        if (tuple.isEmpty()) {
            return zero();
        }

        return TridentUtils.valueFromTuple(tuple, inputFieldName);
    }

    @Override
    public Number combine(Number val1, Number val2) {
        return (Number) Numbers.max(val1, val2);
    }

    @Override
    public Number zero() {
        return Double.MIN_VALUE;
    }
}
