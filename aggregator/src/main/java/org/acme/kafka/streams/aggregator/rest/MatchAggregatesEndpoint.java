package org.acme.kafka.streams.aggregator.rest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.acme.kafka.streams.aggregator.streams.QueryResult;
import org.jboss.logging.Logger;
import org.acme.kafka.streams.aggregator.streams.InteractiveQueries;
import org.acme.kafka.streams.aggregator.streams.PipelineMetadata;

@ApplicationScoped
@Path("/games")
public class MatchAggregatesEndpoint {
    private static final Logger LOG = Logger.getLogger(MatchAggregatesEndpoint.class);

    String serviceName = System.getenv("SERVICE_NAME");

    @Inject
    InteractiveQueries interactiveQueries;

    @GET
    @Path("/{gameId}/matches/{matchId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlayerMatches(@PathParam("gameId") String gameId, @PathParam("matchId") String matchId) {
        String id = gameId + ":" + matchId;

        QueryResult result = interactiveQueries.getPlayerMatchesStore(id);
        LOG.debug("result for " + id + " is on host:" + result.getHost());
        LOG.debug("result is present:" + result.getResult().isPresent());
        if (result.getResult().isPresent()) {
            LOG.debug("returning result from self");
            return Response.ok(result.getResult().get()).build();
        } else if (result.getHost().isPresent()) {
            URL url = getOtherUrl(result.getHost().get(), result.getPort().getAsInt(), gameId, matchId);
            LOG.debug("get for key/id was found in node at URL: " + url.toString());
            try {
                HttpURLConnection conn = (HttpURLConnection)url.openConnection();
                conn.setRequestMethod("GET");
                conn.setRequestProperty("User-Agent","Mozilla/5.0 ( compatible ) ");
                conn.setRequestProperty("Accept","*/*");
                conn.setDoOutput(true);
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);

                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();

                return Response.ok(content).build();
            } catch (Exception e) {
                LOG.error("error fetching: " + url.toString());
                e.printStackTrace();
                return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode(), "{ \"info\": \"Error fetching data from other Kafka Streams node\" }").build();
            }
        } else {
            String msg = "no data found for match " + matchId + " from game " + gameId;
            LOG.info(msg);
            return Response.status(Status.NOT_FOUND.getStatusCode(), msg).build();
        }
    }

    @GET
    @Path("/meta-data")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PipelineMetadata> getMetaData() {
        return interactiveQueries.getMetaData();
    }

    /**
     * When running in a Kubernetes/OpenShift StatefulSet we need to include
     * the headless service name when creating the hostname.
     */
    private String getPodHostname (String host) {
        if (serviceName != null) {
            return host + "." + serviceName;
        } else {
            return host;
        }
    }

    private URL getOtherUrl(String host, int port, String gameId, String matchId) {
        try {
            return new URL("http://" + getPodHostname(host) + ":" + port + "/games/" + gameId + "/matches/" + matchId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
