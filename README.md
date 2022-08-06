<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Beam Crime Analysis

The code in this repository serve to demonstrate running Beam pipelines with SamzaRunner locally, in Yarn cluster,
or in standalone cluster with Zookeeper.

### Example Pipelines
The following examples are included:

1. [`CrimeAnalysis`](https://github.com/szkudlarekdamian/beam-crime-analysis/blob/master/src/main/java/org/crime/CrimeAnalysis.java) reads a file as input (bounded data source), and computes firstly the number of crimes commited each hour of the day and, secondly the number of crimes committed in private and public schools. 

2. [`CrimeAnalysisStream`](https://github.com/szkudlarekdamian/beam-crime-analysis/blob/master/src/main/java/org/crime/CrimeAnalysisStream.java) reads from a Kafka stream (unbounded data source). It uses a sliding 7-day window, with 1-day interval to aggregate the events, and calculates selected statistics on crimes committed in each Chicago district.

### Run the Analysis

Each example can be run locally, in Yarn cluster or in standalone cluster.

#### Set Up
1. Download and install [JDK version 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html). Verify that the JAVA_HOME environment variable is set and points to your JDK installation.

2. Download and install [Apache Maven](http://maven.apache.org/download.cgi) by following Maven’s [installation guide](http://maven.apache.org/install.html) for your specific operating system.

A script named "grid" is included in this project which allows you to easily download and install Zookeeper, Kafka, and Yarn.
You can run the following to bring them all up running in your local machine:

```
$ scripts/grid bootstrap
```

All the downloaded package files will be put under `deploy` folder. Once the grid command completes, 
you can verify that Yarn is up and running by going to http://localhost:8088. You can also choose to
bring them up separately, e.g.:

```
$ scripts/grid install zookeeper
$ scripts/grid start zookeeper
```
   
#### Run Locally
You can run directly within the project using maven:

```
$ mvn compile exec:java -Dexec.mainClass=org.crime.CrimeAnalysis \
    -Dexec.args="--runner=SamzaRunner --experiments=use_deprecated_read" -P samza-runner
```

#### Packaging Your Application
To execute the example in either Yarn or standalone, you need to package it first.
After packaging, we deploy and explode the tgz in the deploy folder:

```
 $ mkdir -p deploy/examples
 $ mvn package && tar -xvf target/beam-crime-analysis-0.1-dist.tar.gz -C deploy/examples/
```

#### Run in Standalone Cluster with Zookeeper
You can use the `run-beam-standalone.sh` script included in this repo to run an example
in standalone mode. The config file is provided as `config/standalone.properties`. Note by
default we create one single split for the whole input (--maxSourceParallelism=1). To 
set each Kafka partition in a split, we can set a large "maxSourceParallelism" value which 
is the upper bound of the number of splits.

```
$ deploy/examples/bin/run-beam-standalone.sh org.crime.CrimeAnalysisStream \
    --configFilePath=$PWD/deploy/examples/config/standalone.properties --maxSourceParallelism=1024
```

#### Run Yarn Cluster
Similar to running standalone, we can use the `run-beam-yarn.sh` to run the examples
in Yarn cluster. The config file is provided as `config/yarn.properties`. To run the 
KafkaWordCount example in yarn:

```
$ deploy/examples/bin/run-beam-yarn.sh org.crime.CrimeAnalysisStream \
    --configFilePath=$PWD/deploy/examples/config/yarn.properties --maxSourceParallelism=1024
```

### More Information

* [Apache Beam](http://beam.apache.org)
* [Apache Samza](https://samza.apache.org/)
* Quickstart: [Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), [Go](https://beam.apache.org/get-started/quickstart-go)