# Spark-Implementing-Decision-Tree-Learning

A implementation of the ID3 decision tree learning algorithm in Spark. The implementation is
required to work on our cluster configuration set up at the Software Languages Lab (Isabelle).

## Getting Started

This project could be run locally on Intelli J IDEA.

For running on Cluster, a jar file is needed here, detailed is in the Prerequisites and Installing

### Prerequisites

```
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0"
```

Data could be found here: https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt


### Installing

Run it locally: drag sample data and run it on Intelli J IDEA

Run it on cluster(Jar file gernerated): 

Change main class file name in build.sbt to the file name you have

for example: 
```
mainClass in Compile := Some("BigDataProject")
```
```
mainClass in assembly := Some("BigDataProject")
```

And run the following command in sbt shell

```
clean
compile
assembly
```

You will then found a jar file named project.jar in your project folder.


## Acknowledgments

* Class project from Vrije Universiteit Brussels CCBD 2019~2020
