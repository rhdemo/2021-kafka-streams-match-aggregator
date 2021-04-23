package org.acme.kafka.streams.aggregator.model;

import org.jboss.logging.Logger;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@RegisterForReflection
public class Aggregate {
    final public static String INCOMING_KEY_TS = "ts";
    final public static String INCOMING_KEY_GAME_ID = "game";
    final public static String INCOMING_KEY_MATCH_ID = "match";
    final public static String INCOMING_KEY_PLAYER_A = "playerA";
    final public static String INCOMING_KEY_PLAYER_B = "playerB";
    final public static String INCOMING_KEY_PLAYER_UUID = "uuid";
    final public static String INCOMING_KEY_PLAYER_USERNAME = "username";
    final public static String INCOMING_KEY_PLAYER_BOARD = "board";
    final public static String INCOMING_KEY_WINNER = "winner";
    final public static String INCOMING_KEY_ATTACK_BY = "by";
    final public static String INCOMING_KEY_ATTACK_AGAINST = "against";
    final public static String INCOMING_KEY_ATTACK_ORIGIN = "origin";
    final public static String INCOMING_KEY_ATTACK_HIT = "hit";
    final public static String INCOMING_KEY_ATTACK_DESTROYED = "destroyed";

    final public static String AGGREGATE_KEY_GAME_ID = "gameId";
    final public static String AGGREGATE_KEY_MATCH_ID = "matchId";
    final public static String AGGREGATE_KEY_TS_START = "startTs";
    final public static String AGGREGATE_KEY_PLAYER_A = "playerA";
    final public static String AGGREGATE_KEY_PLAYER_B = "playerB";
    final public static String AGGREGATE_KEY_PLAYER_UUID = "uuid";
    final public static String AGGREGATE_KEY_PLAYER_USERNAME = "username";
    final public static String AGGREGATE_KEY_TS_END = "endTs";
    final public static String AGGREGATE_KEY_WINNER = "winner";
    final public static String AGGREGATE_KEY_TURNS = "turns";

    final public static String AGGREGATE_TURNS_KEY_ORIGIN = "origin";
    final public static String AGGREGATE_TURNS_KEY_DESTROYED = "destroyed";
    final public static String AGGREGATE_TURNS_KEY_HIT = "hit";
    final public static String AGGREGATE_TURNS_KEY_ATTACKER = "attacker";
    final public static String AGGREGATE_TURNS_KEY_UPDATED_BOARD = "updatedBoard";

    final public static String PAYLOAD_START = "start";
    final public static String PAYLOAD_END = "end";
    final public static String PAYLOAD_ATTACK = "attack";

    private static final Logger LOG = Logger.getLogger(Aggregate.class);

    public JsonObject match;

    public static JsonObject processIncomingPayload (JsonObject incoming, JsonObject aggregate) {
        String type = incoming.getString("type");
        JsonObject data = incoming.getJsonObject("data");

        LOG.debug("processing payload:");
        LOG.debug(incoming.toString());

        if (type.equals(PAYLOAD_START)) {
            LOG.debug("received match-start payload");

            // Record basic match metadata
            aggregate.put(AGGREGATE_KEY_GAME_ID, data.getString(INCOMING_KEY_GAME_ID));
            aggregate.put(AGGREGATE_KEY_MATCH_ID, data.getString(INCOMING_KEY_MATCH_ID));
            aggregate.put(AGGREGATE_KEY_TS_START, data.getLong(INCOMING_KEY_TS));
            aggregate.put(AGGREGATE_KEY_PLAYER_A, data.getJsonObject(INCOMING_KEY_PLAYER_A));
            aggregate.put(AGGREGATE_KEY_PLAYER_B, data.getJsonObject(INCOMING_KEY_PLAYER_B));
            aggregate.put(AGGREGATE_KEY_TURNS, new JsonArray());
        } else if (type.equals(PAYLOAD_END)){
            LOG.debug("received match-end payload");

            // Note the winner's UUID
            aggregate.put(
                AGGREGATE_KEY_WINNER,
                data.getJsonObject(INCOMING_KEY_WINNER).getString(INCOMING_KEY_PLAYER_UUID)
            );

            // Record the match end timestamp
            aggregate.put(AGGREGATE_KEY_TS_END, data.getLong(INCOMING_KEY_TS));
        } else if (type.equals(PAYLOAD_ATTACK)) {
            LOG.debug("received attack payload");

            JsonArray turnsArray = aggregate.getJsonArray(AGGREGATE_KEY_TURNS);
            JsonObject turnObject = new JsonObject();

            if (turnsArray == null) {
                turnsArray = new JsonArray();
            }

            turnObject.put(AGGREGATE_TURNS_KEY_DESTROYED, data.getString(INCOMING_KEY_ATTACK_DESTROYED));
            turnObject.put(AGGREGATE_TURNS_KEY_HIT, data.getBoolean(INCOMING_KEY_ATTACK_HIT));
            turnObject.put(AGGREGATE_TURNS_KEY_ORIGIN, data.getString(INCOMING_KEY_ATTACK_ORIGIN));
            turnObject.put(AGGREGATE_TURNS_KEY_ATTACKER, data.getJsonObject(INCOMING_KEY_ATTACK_BY).getString(INCOMING_KEY_PLAYER_UUID));
            turnObject.put(
                AGGREGATE_TURNS_KEY_UPDATED_BOARD,
                data
                    .getJsonObject(INCOMING_KEY_ATTACK_AGAINST)
                    .getJsonObject(INCOMING_KEY_PLAYER_BOARD)
            );

            turnsArray.add(turnObject);
        } else {
            LOG.warn("received unknown payload type \"" + type + "\". Not updating aggregate.");
        }

        LOG.debug("updated aggregate JSON: " + aggregate.encode());

        return aggregate;
    }
}
