package org.acme.kafka.streams.aggregator.streams;

import java.util.Optional;
import java.util.OptionalInt;


public class QueryResult {

    private static QueryResult NOT_FOUND = new QueryResult(null, null, null);

    private final String result;
    private final String host;
    private final Integer port;

    private QueryResult(String result, String host, Integer port) {
        this.result = result;
        this.host = host;
        this.port = port;
    }

    public static QueryResult found(String data) {
        return new QueryResult(data, null, null);
    }

    public static QueryResult foundRemotely(String host, int port) {
        return new QueryResult(null, host, port);
    }

    public static QueryResult notFound() {
        return NOT_FOUND;
    }

    public Optional<String> getResult() {
        return Optional.ofNullable(result);
    }

    public Optional<String> getHost() {
        return Optional.ofNullable(host);
    }

    public OptionalInt getPort() {
        return port != null ? OptionalInt.of(port) : OptionalInt.empty();
    }
}
