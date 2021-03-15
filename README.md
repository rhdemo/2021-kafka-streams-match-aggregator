# 2021 Kafka Streams Match Aggregator

This application will receive match events via a Kafka Topic, and aggregate
them into complete records over time. A completely aggregated match object is
a large JSON payload - an example can be [found here](https://gist.github.com/evanshortiss/a70f3ca9710663734223698b0e8ed6cc).

## HTTP API

Aggregated records can be retrieved by the HTTP API that this service exposes.
A known game ID and match ID are required to fetch a record.

```bash
export GAME_ID=c538ddcce22a3a58
export MATCH_ID=OUc5u3ZdLtVw2A8nYWFap

curl http://localhost:8080/games/$GAME_ID/matches/$MATCH_ID
```

## Building

To build the _aggregator_ application, run

```bash
./mvnw package
```

To build a Docker container image:

```bash
./scripts/build.sh
```

## Running

```
./scripts/build.sh

export KAFKA_SVC_USERNAME=username
export KAFKA_SVC_PASSWORD=username
export KAFKA_BOOTSTRAP_URL=hostname.kafka.devshift.org:443

./scripts/run.sh
```


## Scaling

Kafka Streams pipelines can be scaled out, i.e. the load can be distributed amongst multiple application instances running the same pipeline.
To try this out, scale the _aggregator_ service to three nodes:

```bash
docker-compose up -d --scale aggregator=3
```

This will spin up two more instances of this service.
The state store that materializes the current state of the streaming pipeline
(which we queried before using the interactive query),
is now distributed amongst the three nodes.
I.e. when invoking the REST API on any of the three instances, it might either be
that the aggregation for the requested weather station id is stored locally on the node receiving the query,
or it could be stored on one of the other two nodes.

As the load balancer of Docker Compose will distribute requests to the _aggregator_ service in a round-robin fashion,
we'll invoke the actual nodes directly.
The application exposes information about all the host names via REST:

```bash
http aggregator:8080/weather-stations/meta-data
```

Retrieve the data from one of the three hosts shown in the response
(your actual host names will differ):

```bash
http cf143d359acc:8080/weather-stations/data/1
```

If that node holds the data for key "1", you'll get a response like this:

```
HTTP/1.1 200 OK
Connection: keep-alive
Content-Length: 74
Content-Type: application/json
Date: Tue, 11 Jun 2019 19:16:31 GMT

{
    "avg": 15.7,
    "count": 11,
    "max": 31.0,
    "min": 3.3,
    "stationId": 1,
    "stationName": "Hamburg"
}
```

Otherwise, the service will send a redirect:

```
HTTP/1.1 303 See Other
Connection: keep-alive
Content-Length: 0
Date: Tue, 11 Jun 2019 19:17:51 GMT
Location: http://72064bb97be9:8080/weather-stations/data/2
```

You can have _httpie_ automatically follow the redirect by passing the `--follow option`:

```bash
http --follow cf143d359acc:8080/weather-stations/data/2
```

## Running in native

To run the _aggregator_ application as native binary via GraalVM,
first run the Maven builds using the `native` profile:

```bash
mvn clean install -Pnative -Dnative-image.container-runtime=docker
```

Then create an environment variable named `QUARKUS_MODE` and with value set to "native":

```bash
export QUARKUS_MODE=native
```

Now start Docker Compose as described above.

## Running locally

For development purposes it can be handy to run _aggregator_ application
directly on your local machine instead of via Docker.
For that purpose, a separate Docker Compose file is provided which just starts Apache Kafka and ZooKeeper, _docker-compose-local.yaml_
configured to be accessible from your host system.
Open this file an editor and change the value of the `KAFKA_ADVERTISED_LISTENERS` variable so it contains your host machine's name or ip address.
Then run:

```bash
docker-compose -f docker-compose-local.yaml up

mvn quarkus:dev -Dquarkus.http.port=8081 -f aggregator/pom.xml
```

Any changes done to the _aggregator_ application will be picked up instantly,
and a reload of the stream processing application will be triggered upon the next Kafka message to be processed.
