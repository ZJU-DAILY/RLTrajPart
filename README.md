# RLTrajPart

RLTrajPart is a reinforcement learning driven elastic partitioning framework for distributed trajectory stream analytics.

## System requirements

- Java 8
- Apache Flink 1.17
- Kafka 3.4.0
- Python.version = 3.8.19
- PyTorch.version = 2.3.0

The system contains two main components: the trajectory stream analytics on the Flink side, and the reinforcement learning agent implemented with PyTorch.

For the Flink side, compile the Java code with `mvn package`. For the RL agent, install the required Python packages in `RL/requirements.txt`.

## Run

The parameters are configured in `TrajectoryFlink\src\main\resources\conf\trajectoryflink-conf.yml` and `RL/config.py` respectively.

### Offline data collection

1.  Run `python/RL/train.py` to start the RL agent server.
2.  Run the Java class `job.TraSimSearch/TraRangeQuery/TraCluSearch` to collect the offline data.
3.  Run `python/RL/offlineData.py` to process the offline data.

### Online process

1. Run `python/RL/test.py` to start the RL agent server.
2. Run the Java class `job.TraSimSearch/TraRangeQuery/TraCluSearch` to start the online process.

## Datasets

We use two real-world datasets: [T-Drive](https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/), [Chengdu Taxi](https://drive.google.com/file/d/1JDVDeEq7chFuGwVL7Eu3tE_SWbA061cd/view?usp=sharing) and one synthetic dataset: [BrinkHoff](https://iapg.jade-hs.de/en/members/brinkhoff/generator).

