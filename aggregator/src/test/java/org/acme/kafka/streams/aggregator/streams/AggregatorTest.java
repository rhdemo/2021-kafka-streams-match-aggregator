package org.acme.kafka.streams.aggregator.streams;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.acme.kafka.streams.aggregator.model.Aggregate;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonObject;


/**
 * Integration testing of the application with an embedded broker.
 */
@QuarkusTest
@QuarkusTestResource(KafkaResource.class)
public class AggregatorTest {

    private static final Logger LOG = Logger.getLogger(AggregatorTest.class);

    String matchStartJson;
    String attackJson;

    @BeforeEach
    public void setUp() throws Exception {

        attackJson = readFileAsString("src/test/resources/attack.json");
    }

    @AfterEach
    public void tearDown() {

    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testMatchStartAggregation() throws InterruptedException {
        JsonObject incoming = new JsonObject(matchStartJson);
        JsonObject aggregate = Aggregate.processIncomingPayload(incoming, new JsonObject());

        LOG.info("aggregate:");
        LOG.info(aggregate.encode());
        LOG.info("incoming:");
        LOG.info(incoming.encode());

        Assertions.assertEquals(
            aggregate.getString(Aggregate.AGGREGATE_KEY_GAME_ID),
            incoming.getJsonObject("data").getString("game")
        );
        Assertions.assertEquals(
            aggregate.getString(Aggregate.AGGREGATE_KEY_MATCH_ID),
            incoming.getJsonObject("data").getString("match")
        );
        Assertions.assertEquals(
            aggregate
                .getJsonObject(Aggregate.AGGREGATE_KEY_PLAYER_A)
                .getString(Aggregate.AGGREGATE_KEY_PLAYER_UUID),
            incoming.getJsonObject("data").getJsonObject("playerA").getString("uuid")
        );
        Assertions.assertEquals(
            aggregate
                .getJsonObject(Aggregate.AGGREGATE_KEY_PLAYER_A)
                .getString(Aggregate.AGGREGATE_KEY_PLAYER_USERNAME),
            incoming.getJsonObject("data").getJsonObject("playerA").getString("username")
        );
        Assertions.assertEquals(
            aggregate
                .getJsonObject(Aggregate.AGGREGATE_KEY_PLAYER_B)
                .getString(Aggregate.AGGREGATE_KEY_PLAYER_UUID),
            incoming.getJsonObject("data").getJsonObject("playerB").getString("uuid")
        );
        Assertions.assertEquals(
            aggregate
                .getJsonObject(Aggregate.AGGREGATE_KEY_PLAYER_B)
                .getString(Aggregate.AGGREGATE_KEY_PLAYER_USERNAME),
            incoming.getJsonObject("data").getJsonObject("playerB").getString("username")
        );
        Assertions.assertEquals(
            aggregate.getJsonArray(Aggregate.AGGREGATE_KEY_TURNS).getList().size(),
            0
        );
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testAttackAggregation() throws InterruptedException {
        JsonObject initialAggregation = Aggregate.processIncomingPayload(
            new JsonObject(matchStartJson),
            new JsonObject()
        );

        LOG.info("initial aggregate:");
        LOG.info(initialAggregation.encode());

        JsonObject attackAggregate = Aggregate.processIncomingPayload(
            new JsonObject(attackJson),
            initialAggregation
        );

        LOG.info("attack aggregate:");
        LOG.info(attackAggregate.encode());

        Assertions.assertEquals(
            attackAggregate.getString(Aggregate.AGGREGATE_KEY_GAME_ID),
            initialAggregation.getString(Aggregate.AGGREGATE_KEY_GAME_ID)
        );
        Assertions.assertEquals(
            attackAggregate.getString(Aggregate.AGGREGATE_KEY_MATCH_ID),
            initialAggregation.getString(Aggregate.AGGREGATE_KEY_MATCH_ID)
        );
        Assertions.assertEquals(
            attackAggregate.getJsonArray(Aggregate.AGGREGATE_KEY_TURNS).getList().size(),
            1
        );
        JsonObject turn = attackAggregate.getJsonArray(Aggregate.AGGREGATE_KEY_TURNS).getJsonObject(0);
        Assertions.assertEquals(
            turn.getString(Aggregate.AGGREGATE_TURNS_KEY_ATTACKER),
            new JsonObject(attackJson).getJsonObject("data").getJsonObject(Aggregate.INCOMING_KEY_ATTACK_BY).getString(Aggregate.INCOMING_KEY_PLAYER_UUID)
        );
        Assertions.assertEquals(
            turn.getString(Aggregate.AGGREGATE_TURNS_KEY_ORIGIN),
            new JsonObject(attackJson).getJsonObject("data").getString(Aggregate.INCOMING_KEY_ATTACK_ORIGIN)
        );
    }

    private static String readFileAsString(String file)throws Exception {
        return new String(Files.readAllBytes(Paths.get(file)));
    }
}
