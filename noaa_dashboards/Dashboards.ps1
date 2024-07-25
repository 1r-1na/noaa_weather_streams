docker build -t noaa_dashboards .
docker run -dp 8050:8050 --restart unless-stopped --name noaa_dashboards noaa_dashboards