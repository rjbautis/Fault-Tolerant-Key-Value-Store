# Fault Tolerant and Scalable Key-Value Store
A [Fault Tolerant and Scalable Key-Value Store](hw4solution.py), written all in Python 3.0, as a final project for the Distributed Systems undergraduate course at UC Santa Cruz. Simulates node replication across multiple partitions, utilizes consistent hashing for key distribution, and runs background services to ensure fault tolerance in the face of network disconnections. 

Built with Flask and tested using Docker.

Disclaimer: HW4 spec file and unit test written by TA Umang Sardesai (Winter Quarter 2018), not by me.

### Prerequisites

Things you need to run the KVS and where to download them

* [Python 2.7.10](https://www.python.org/download/releases/2.7/) - The language used for test script (not for source code)
* [Docker](https://docs.docker.com/install/) - Docker containers and images
* [Requests](http://docs.python-requests.org/en/master/) - For running the code and test script

### Installing

Download/clone the repository onto your local machine and turn on Docker. Be sure to check that [requirements.txt](requirements.txt) includes Flask and Requests. From the current directory where [Dockerfile](Dockerfile) exists, run the following line:

```
docker build -t hw4 .
```
This creates a Docker image that installs all dependencies, runs the KVS written in Python 3.0, and exposes port 8080 for containers (image instances) to listen for connections. 

Now create a network in docker:

```
docker network create --subnet=10.0.0.0/16 mynet
```

We are ready to begin instantiating our docker containers. For example, run the following lines to create a KVS with 4 nodes and 2 partitions (taken from [HW4_Spec.md](spec/HW4_Spec.md)):

```
docker run -p 8081:8080 --ip=10.0.0.21 --net=mynet -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080" -e ip_port="10.0.0.21:8080" hw4
docker run -p 8082:8080 --ip=10.0.0.22 --net=mynet -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080" -e ip_port="10.0.0.22:8080" hw4
docker run -p 8083:8080 --ip=10.0.0.23 --net=mynet -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080" -e ip_port="10.0.0.23:8080" hw4
docker run -p 8084:8080 --ip=10.0.0.24 --net=mynet -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080" -e ip_port="10.0.0.24:8080" hw4
```

## Running the tests

You may test the KVS yourself with GET and PUT requests. Note that POST and DELETE requests have not been covered yet.

GET request to return number of keys inside the current partition
```
curl -X GET localhost:8081/kvs/get_number_of_keys
```

PUT request to insert a new key into the overall system.
```
curl -X PUT localhost:8081/kvs -d "key=foo&value=bar&causal_payload="
```
Please note that every new key inserted must be inserted with an empty *causal_payload*, but from that point on the user must remember the returned value for future requests (GET requests will not increment *causal_payload*, but PUT requests will)

**To run the provided unit test, run the following:**
```
python test_HW4.py
```

## Built With

* [Flask](http://flask.pocoo.org/) - The web framework used
* [Requests](http://docs.python-requests.org/en/master/) - For making HTTP requests

## Authors

* **Ryan Bautista**
* **Brandon Samudio**

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Professor Peter Alvaro, Distributed Systems Course 2018
* TA Umang Sardesai
