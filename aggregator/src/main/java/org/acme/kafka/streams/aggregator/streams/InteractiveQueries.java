package org.acme.kafka.streams.aggregator.streams;

import java.util.List;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class InteractiveQueries {

    private static final Logger LOG = Logger.getLogger(InteractiveQueries.class);

    @ConfigProperty(name = "hostname")
    String host;

    @Inject
    KafkaStreams streams;

    public List<PipelineMetadata> getMetaData() {
        return streams.allMetadataForStore(TopologyProducer.MATCHES_STORE)
                .stream()
                .map(m -> new PipelineMetadata(
                        m.hostInfo().host() + ":" + m.hostInfo().port(),
                        m.topicPartitions()
                                .stream()
                                .map(TopicPartition::toString)
                                .collect(Collectors.toSet())))
                .collect(Collectors.toList());
    }

    public QueryResult getPlayerMatchesStore(String id) {
        StreamsMetadata metadata = streams.metadataForKey(
                TopologyProducer.MATCHES_STORE,
                id,
                Serdes.String().serializer());

        if (metadata == null || metadata == StreamsMetadata.NOT_AVAILABLE) {
            LOG.warnv("Found no metadata for key {0}", id);
            return QueryResult.notFound();
        } else if (metadata.host().equals(host)) {
            LOG.infov("Found data for key {0} locally", id);
            String result = getPlayerMatchesStore().get(id);

            if (result != null) {
                return QueryResult.found(result);
            } else {
                return QueryResult.notFound();
            }
        } else {
            LOG.infov("Found data for key {0} on remote host {1}:{2}", id, metadata.host(), metadata.port());
            return QueryResult.foundRemotely(metadata.host(), metadata.port());
        }
    }

    private ReadOnlyKeyValueStore<String, String> getPlayerMatchesStore () {
        while (true) {
            try {
                return streams.store(TopologyProducer.MATCHES_STORE, QueryableStoreTypes.keyValueStore());
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
}
