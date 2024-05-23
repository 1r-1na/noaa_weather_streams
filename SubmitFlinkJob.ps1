$FileName = "noaa_weather_streams-1.0-SNAPSHOT.jar"
$ContainerId = "noaa_weather_streams-jobmanager-1"

docker exec ${ContainerId} mkdir /app

docker cp ./target/${FileName} ${ContainerId}:/app/${FileName}

docker exec ${ContainerId} flink run --detached /app/${FileName}