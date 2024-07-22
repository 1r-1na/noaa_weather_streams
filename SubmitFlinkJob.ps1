docker compose down
docker compose up -d --build

$FileName = "noaa_weather_streams-1.0-SNAPSHOT.jar"
$JobManagerContainerId  = "noaa_weather_streams-jobmanager-1"
$TaskManagerJobId = "noaa_weather_streams-taskmanager-1"
$MainClassPath = "at.fhv.streamprocessing.flink.WeatherDataJob"


# use maven to build the app (create a persistent volume to cache dependencies)
docker volume create --name maven-repo
docker run -it --rm --name ${FileName}.ToLower() -v maven-repo:/root/.m2 -v ${PWD}:/usr/src/mymaven -w /usr/src/mymaven maven:3.9.6-eclipse-temurin-11 mvn package

# create app dir in jobmanager to store job jar
docker exec ${JobManagerContainerId} mkdir /app

# upload new jar
docker cp target/${FileName} ${JobManagerContainerId}:/app/$FileName

# start new job
docker exec ${JobManagerContainerId} flink run --detached -m localhost:8081 -c ${MainClassPath} /app/${FileName}

# attach to logger of taskmanager to see whats going on
docker logs -f ${TaskManagerJobId}