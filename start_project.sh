echo "Start"
sudo service docker start

docker start redpanda

docker start mongodb

echo "Cont."
docker ps
