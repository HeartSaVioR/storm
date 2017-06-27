package org.apache.storm.state;

/**
 * The interface of State Encoder.
 */
public interface StateEncoder<K, V, KENCODED, VENCODED> {
    /**
     * Encode key.
     *
     * @param key the value of key (K type)
     * @return the encoded value of key (KENCODED type)
     */
    KENCODED encodeKey(K key);

    /**
     * Encode value.
     *
     * @param value the value of value (V type)
     * @return the encoded value of value (VENCODED type)
     */
    VENCODED encodeValue(V value);

    /**
     * Decode key.
     *
     * @param encodedKey the value of key (KRAW type)
     * @return the decoded value of key (K type)
     */
    K decodeKey(KENCODED encodedKey);

    /**
     * Decode value.
     *
     * @param encodedValue the value of key (VENCODED type)
     * @return the decoded value of key (V type)
     */
    V decodeValue(VENCODED encodedValue);

    /**
     * Get the tombstone value (deletion mark).
     *
     * @return the tomestone value (VENCODED type)
     */
    VENCODED getTombstoneValue();
}
