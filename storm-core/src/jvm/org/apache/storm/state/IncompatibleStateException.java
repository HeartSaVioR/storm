package org.apache.storm.state;

/**
 * This exception represents that stored State is not compatible with provided KeyValueState implementation.
 */
public class IncompatibleStateException extends RuntimeException {
    public IncompatibleStateException(String message) {
        super(message);
    }
}
