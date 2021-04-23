package org.acme.kafka.streams.aggregator.streams;

import static org.acme.kafka.streams.aggregator.streams.TopologyProducer.MATCHES_TOPIC;
import static org.acme.kafka.streams.aggregator.streams.TopologyProducer.AGGREGATE_TOPIC;
import static org.acme.kafka.streams.aggregator.streams.TopologyProducer.MATCHES_STORE;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonObject;

/**
 * Testing of the Topology without a broker, using TopologyTestDriver
 */
@QuarkusTest
public class TopologyProducerTest {

    @Inject
    Topology topology;

    TopologyTestDriver testDriver;

    TestInputTopic<String, String> matches;

    TestOutputTopic<String, String> aggregates;

    JsonObject matchStartPayload;
    JsonObject attackPayload;
    JsonObject matchEndPayload;

    private static final Logger LOG = Logger.getLogger(TopologyProducerTest.class);

    @BeforeEach
    public void setUp() throws Exception {

        matchStartPayload = new JsonObject(readFileAsString("src/test/resources/match-start.json"));
        attackPayload = new JsonObject(readFileAsString("src/test/resources/attack.json"));
        matchEndPayload = new JsonObject(readFileAsString("src/test/resources/match-end.json"));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testApplicationId");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(topology, config);

        matches = testDriver.createInputTopic(MATCHES_TOPIC, new StringSerializer(), new StringSerializer());
        aggregates = testDriver.createOutputTopic(AGGREGATE_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    public void tearDown(){
        testDriver.getTimestampedKeyValueStore(MATCHES_STORE).flush();
        testDriver.close();
    }

    @Test
    public void testTopology(){
        matches.pipeInput(
            createKeyFromJson(matchStartPayload),
            matchStartPayload.encode()
        );
        matches.pipeInput(
            createKeyFromJson(attackPayload),
            attackPayload.encode()
        );
        matches.pipeInput(
            createKeyFromJson(matchEndPayload),
            matchEndPayload.encode()
        );

        TestRecord<String, String> start = aggregates.readRecord();
        TestRecord<String, String> mid = aggregates.readRecord();
        TestRecord<String, String> end = aggregates.readRecord();

        LOG.info(start.getKey() + ":" + start.getValue());
        LOG.info(mid.getKey() + ":" + mid.getValue());
        LOG.info(end.getKey() + ":" + end.getValue());
    }

    private static String readFileAsString(String file)throws Exception {
        return new String(Files.readAllBytes(Paths.get(file)));
    }

    private String createKeyFromJson (JsonObject json) {
        LOG.info("Get key from: " + json.encode());
        return json.getJsonObject("data").getString("game") + ":" + json.getJsonObject("data").getString("match");
    }
}
