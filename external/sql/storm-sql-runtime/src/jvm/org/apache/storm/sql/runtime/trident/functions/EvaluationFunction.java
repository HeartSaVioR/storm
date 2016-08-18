package org.apache.storm.sql.runtime.trident.functions;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class EvaluationFunction extends BaseFunction {
    private static final Logger LOG = LoggerFactory.getLogger(EvaluationFunction.class);

    private transient ScriptEvaluator evaluator;
    private final String expression;

    public EvaluationFunction(String expression) {
        this.expression = expression;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        LOG.info("Expression: {}", expression);
        try {
            evaluator = new ScriptEvaluator(expression, Values.class,
                    new String[] {"_data"}, new Class[] { List.class });
        } catch (CompileException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            collector.emit((Values) evaluator.evaluate(new Object[] {tuple.getValues()}));
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
