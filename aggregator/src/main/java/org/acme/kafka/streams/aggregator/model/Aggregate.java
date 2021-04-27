package org.acme.kafka.streams.aggregator.model;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@RegisterForReflection
public class Aggregate {
    final public static String INCOMING_KEY_TS = "ts";
    final public static String INCOMING_KEY_CLUSTER = "cluster";
    final public static String INCOMING_KEY_GAME_ID = "game";
    final public static String INCOMING_KEY_MATCH_ID = "match";
    final public static String INCOMING_KEY_PLAYER_A = "playerA";
    final public static String INCOMING_KEY_PLAYER_B = "playerB";
    final public static String INCOMING_KEY_PLAYER_UUID = "uuid";
    final public static String INCOMING_KEY_SCORE = "score";
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
    final public static String AGGREGATE_KEY_SCORE = "score";
    final public static String AGGREGATE_KEY_TS_START = "startTs";
    final public static String AGGREGATE_KEY_PLAYER_A = "playerA";
    final public static String AGGREGATE_KEY_PLAYER_B = "playerB";
    final public static String AGGREGATE_KEY_PLAYER_UUID = "uuid";
    final public static String AGGREGATE_KEY_PLAYER_USERNAME = "username";
    final public static String AGGREGATE_KEY_TS_END = "endTs";
    final public static String AGGREGATE_KEY_WINNER = "winner";
    final public static String AGGREGATE_KEY_TURNS = "turns";
    final public static String AGGREGATE_KEY_CLUSTER = "cluster";


    final public static String AGGREGATE_TURNS_KEY_ORIGIN = "origin";
    final public static String AGGREGATE_TURNS_KEY_DESTROYED = "destroyed";
    final public static String AGGREGATE_TURNS_KEY_HIT = "hit";
    final public static String AGGREGATE_TURNS_KEY_ATTACKER = "attacker";
    final public static String AGGREGATE_TURNS_KEY_UPDATED_BOARD = "updatedBoard";

    final public static String PAYLOAD_START = "start";
    final public static String PAYLOAD_END = "end";
    final public static String PAYLOAD_ATTACK = "attack";

    private static final Logger LOG = Logger.getLogger(Aggregate.class);
    private static final String REPLAY_TRACKER_ENDPOINT = System.getenv("REPLAY_TRACKER_ENDPOINT");

    @ConfigProperty(name = "send-to-tracker")
    static
    String sendToTracker;

    public JsonObject match;

    public static JsonObject processIncomingPayload (JsonObject incoming, JsonObject aggregate) {
        String type = incoming.getString("type");
        JsonObject data = incoming.getJsonObject("data");

        LOG.debug("processing payload:");
        LOG.debug(incoming.toString());

        if (type.equals(PAYLOAD_START)) {
            LOG.info("received match-start payload");

            // Record basic match metadata
            aggregate.put(AGGREGATE_KEY_TS_START, incoming.getLong(INCOMING_KEY_TS));
            aggregate.put(AGGREGATE_KEY_CLUSTER, incoming.getString(INCOMING_KEY_CLUSTER));

            aggregate.put(AGGREGATE_KEY_GAME_ID, data.getString(INCOMING_KEY_GAME_ID));
            aggregate.put(AGGREGATE_KEY_MATCH_ID, data.getString(INCOMING_KEY_MATCH_ID));
            aggregate.put(AGGREGATE_KEY_PLAYER_A, data.getJsonObject(INCOMING_KEY_PLAYER_A));
            aggregate.put(AGGREGATE_KEY_PLAYER_B, data.getJsonObject(INCOMING_KEY_PLAYER_B));
            aggregate.put(AGGREGATE_KEY_TURNS, new JsonArray());
        } else if (type.equals(PAYLOAD_END)){
            LOG.info("received match-end payload");

            // Note the winner's UUID, score, and match end timestamp
            aggregate.put(
                AGGREGATE_KEY_WINNER,
                data.getJsonObject(INCOMING_KEY_WINNER).getString(INCOMING_KEY_PLAYER_UUID)
            );
            aggregate.put(AGGREGATE_KEY_TS_END, incoming.getLong(INCOMING_KEY_TS));
            aggregate.put(AGGREGATE_KEY_SCORE, data.getInteger(INCOMING_KEY_SCORE));

            postGameToReplayTracker(aggregate);
        } else if (type.equals(PAYLOAD_ATTACK)) {
            LOG.info("received attack payload");

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

    private static void postGameToReplayTracker (JsonObject completeGameRecord) {
        if (sendToTracker == "false") {
            return;
        }

        String key = completeGameRecord.getString(AGGREGATE_KEY_GAME_ID) + ":" + completeGameRecord.getString(AGGREGATE_KEY_MATCH_ID);
        LOG.info("sending complete record for key " + key + " to replay tracker");
        try {
            String svc = REPLAY_TRACKER_ENDPOINT != null ? REPLAY_TRACKER_ENDPOINT : "streams-replay-tracker:8080";
            URL url = new URL("http://" +svc + "/game/replay");

            LOG.debug("sending " + key + " to URL: " + url.toString());

            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("User-Agent","Mozilla/5.0 ( compatible ) ");
            conn.setRequestProperty("Accept","*/*");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            try(OutputStream os = conn.getOutputStream()) {
                byte[] input = completeGameRecord.encode().getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            try(BufferedReader br = new BufferedReader(
            new InputStreamReader(conn.getInputStream(), "utf-8"))) {
                StringBuilder response = new StringBuilder();
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                LOG.debug("response replay tracker was: " + response.toString());
            }
        } catch (Exception e) {
            LOG.error("error posting complete game to replay tracker");
            e.printStackTrace();
        }
    }
}
