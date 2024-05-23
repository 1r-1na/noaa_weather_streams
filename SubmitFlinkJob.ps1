$FileName = "noaa_weather_streams-1.0-SNAPSHOT.jar"
$JobManagerContainerId  = "noaa_weather_streams-jobmanager-1"
$TaskManagerJobId = "noaa_weather_streams-taskmanager-1"
$MainClassPath = "at.fhv.streamprocessing.flink.DemoWeatherDataJob"


# use maven to build the app (create a persistent volume to cache dependencies)
docker volume create --name maven-repo
docker run -it --rm --name ${FileName}.ToLower() -v maven-repo:/root/.m2 -v ${PWD}:/usr/src/mymaven -w /usr/src/mymaven maven:3.9.6-eclipse-temurin-11 mvn package

# create app dir in jobmanager to store job jar
docker exec ${JobManagerContainerId} mkdir /app

# remove old jar if exists
docker exec ${JobManagerContainerId} rm /app/$FileName

# upload new jar
docker cp target/${FileName} ${JobManagerContainerId}:/app/$FileName

# get all currently running jobs and remove them
$ReturnStringWithIds = docker exec ${JobManagerContainerId} flink list
$JobIds = [regex]::Matches($ReturnStringWithIds, '\b[0-9a-f]{32}\b')

foreach ($JobId in $JobIds) {
    echo "[Script] Canceling Job $JobId"
    docker exec ${JobManagerContainerId} flink cancel $JobId
}

# start new job
docker exec ${JobManagerContainerId} flink run --detached -m localhost:8081 -c ${MainClassPath} /app/${FileName}

# attach to logger of taskmanager to see whats going on
docker logs -f ${TaskManagerJobId}