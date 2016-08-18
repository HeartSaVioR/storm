package org.apache.storm.sql.runtime.trident;

import org.apache.storm.trident.tuple.TridentTuple;

public class TridentUtils {
    private TridentUtils() {
    }

    public static <T> T valueFromTuple(TridentTuple tuple, String inputFieldName) {
        // when there is no input field then the whole tuple is considered for comparison.
        Object value;
        if (tuple == null) {
            throw new IllegalArgumentException("Tuple is null");
        }
        return (T) tuple.getValueByField(inputFieldName);
    }
}
