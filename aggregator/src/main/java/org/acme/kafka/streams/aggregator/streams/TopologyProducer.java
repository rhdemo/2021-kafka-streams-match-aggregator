package org.acme.kafka.streams.aggregator.streams;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.acme.kafka.streams.aggregator.model.Aggregate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.jboss.logging.Logger;

import io.vertx.core.json.JsonObject;


@ApplicationScoped
public class TopologyProducer {
    // All match events are written to this topic. These events have varying structure
    // so we do a little magic and filtering below to manage that complexity
    static final String MATCHES_TOPIC = "match-updates";
    static final String AGGREGATE_TOPIC = "matches-aggregated";
    static final String MATCHES_STORE = "matches-store";

    private static final Logger LOG = Logger.getLogger(TopologyProducer.class);

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(MATCHES_STORE);

        builder.stream(
            MATCHES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
            )
            .groupByKey()
            .aggregate(
                String::new,
                (key, value, aggregate) -> {
                    LOG.info("processing record for key: " +  key);
                    JsonObject incomingJson = new JsonObject(value);
                    JsonObject aggregateJson;

                    if (aggregate.length() > 0) {
                        LOG.debug("existing aggregate found, will append");
                        // Create a JSON object from existing aggregate data
                        aggregateJson = new JsonObject(aggregate);
                    } else {
                        // Create a new empty JSON object
                        LOG.debug("no existing aggregate. creating initial JSON object");
                        aggregateJson = new JsonObject();
                    }

                    return Aggregate.processIncomingPayload(incomingJson, aggregateJson).encode();
                },
                Materialized.<String, String>as(storeSupplier)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
            )
            .toStream()
            .to(
                AGGREGATE_TOPIC,
                Produced.with(Serdes.String(), Serdes.String())
            );

        return builder.build();
    }
}
