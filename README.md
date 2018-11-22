## grs

Test project for checking out the capabilities of the Redis Stream. Does this by introducing a publisher and consumer microservice.

To run the project, use the supplied docker-compose.yaml file by running the following command:
```
$ docker-compose up
```

This command will continue printing logs from all the services and you will be able to keep track of which consumer
is consuming which entry.

If you would like to test out the scaling capabilities of the consumers, in a different terminal use this command
to change the number of consumers (in this case, 3) running at the same time:
```
$ docker-compose up -d --scale consumer=3
```

If you want to run the services individually, you can build them from the `cmd/{service}` directory and run
manually with the options specified below. By default, both services will require a Redis server instance running
on localhost.


### Publisher

Publisher implements a endpoint which expects an entry in JSON format and stores it in the entries Redis stream

#### Endpoints

`POST /entry`

Body: 
```
{"object_id":3, "object_type":2, "action":"create", "meta":"JSON"}
```

Sample `curl` request:
```
$ curl -d '{"object_id":3, "object_type":2, "action":"create", "meta":"JSON"}' http://localhost:8808/entry
```

**Response**

HTTP status code 201 (Created) for successful requests with valid body.
HTTP status code 400 (Bad request) for requests with unexpected body formats and/or values.
HTTP status code 500 (Internal server error) if something unexpected happens (like unable to read request body).


#### Options with default values
```
--port=80          //HTTP port that the service will listen and serve
--redisAddr=:6379  //Address of the Redis server host
```

### Consumer

Consumer fetches the entries from Redis Stream and consumes them. For this simple test, consuming them means printing them out to standard output in a formatted way. Its also in charge of keeping track where it is at on the stream and is able to pick up reading the stream from the last known position and continue consuming entries. Consumer that is fresh (doesn't pick up work from a previous consumer) start reading new entries from the stream.

#### Options with default values
```
--redisAddr=:6379  //Address of the Redis server host
```
