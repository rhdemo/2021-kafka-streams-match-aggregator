package org.acme.kafka.streams.aggregator.rest;

import java.net.URI;
import java.net.URISyntaxException;
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
import org.acme.kafka.streams.aggregator.streams.InteractiveQueries;

@ApplicationScoped
@Path("/games")
public class MatchAggregatesEndpoint {

    @Inject
    InteractiveQueries interactiveQueries;

    @GET
    @Path("/{gameId}/matches/{matchId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlayerMatches(@PathParam("gameId") String gameId, @PathParam("matchId") String matchId) {
        String id = gameId + ":" + matchId;

        QueryResult result = interactiveQueries.getPlayerMatchesStore(id);
        if (result.getResult().isPresent()) {
            return Response.ok(result.getResult().get()).build();
        } else if (result.getHost().isPresent()) {
            URI otherUri = getOtherUri(result.getHost().get(), result.getPort().getAsInt(), gameId, matchId);
            return Response.seeOther(otherUri).build();
        } else {
            return Response.status(Status.NOT_FOUND.getStatusCode(), "No data found for weather station " + id).build();
        }
    }

    private URI getOtherUri(String host, int port, String gameId, String matchId) {
        try {
            return new URI("http://" + host + ":" + port + "/games/" + gameId + "/matches/" + matchId);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
