## Description

This is a project based on the data and problems in the following repositories:
* https://github.com/EQWorks/ws-data-spark
* https://gist.github.com/woozyking/f1d50e1fe1b3bf52e3748bc280cf941f

## Versions of Tools

SBT version: 1.3.8|
Spark: 2.4.1|
Scala: 2.11.12

## Introduction to the files

* Under the `data` directory, the following files are offered by the above mentioned repositories:  
    `DataSample.csv`  
    `POIList.csv`  
    `relations.txt`  
    `task_ids.txt`  
    `question.txt`  
* `eqworks-problems_2.11-0.1.jar` is the `jar` file after building my project. This file is placed here because it has to be under the same directory with the provided data files in order to be run successfully. You can use the following command to run this file in the provided docker environment:  
    `spark-submit --class "EqworksProblems" /tmp/data/eqworks-problems_2.11-0.1.jar`
* The other directories under `data` are generated after running the `jar` file. They contain the output of the problems. They are described as follows:  
    `data/DataSampleCleaned` contains the output (with a file name ending with `.csv`) of `problem 1. Cleanup`.  
    `data/DataSampleLabeled` contains the output (with a file name ending with `.csv`) of `problem 2. Label`.  
    `data/PoiStat` contains the output (with a file name ending with `.csv`) of `problem 3. Analysis`.  
    `data/schedule` contains the output (with a file name ending with `.csv`) of `problem 4b. Pipeline Dependency`.

## Build Instruction

Although this project has already been built, you can follow the following steps to build it using SBT:
1. Let's say you are now under the `data` directory. Then go to the project directory:  
    `$cd eqworks-problems`
2. Under the project directory `data/eqworks-problems`, run SBT command:  
    `$sbt package`
3. When the build is finished, move the `jar` file to the `data` directory:
    '$cd ..'  
    '$mv eqworks-problems/target/scala-2.11/eqworks-problems_2.11-0.1.jar ./'
4. You can run the `jar` file by running (the output would be overwritten):  
    `spark-submit --class "EqworksProblems" /tmp/data/eqworks-problems_2.11-0.1.jar`

## Short comment on `Problem 2. Label`

  Introduction:  
    * Algorithm: brute force  
    * Time complexity: Θ(M*N), where M is the amount of POIs and N is the number of requests  

  Possible future improvement(It's not implemented due to limited time):  
    * Data structure: K-D tree  
    * Algorithm: nearest neighbour search  
    * Time complexity: O(M\*N) for the worst case, O(M\*lgN) for the average  

## Short comment on `Problem 4b. Pipeline Dependency`

  Introduction:  
    * Data structure: doubly-linked orthogonal list  
    * Algorithm: depth first search (DFS)  
    * Time complexity: O(M), where M is the number of arcs in the DAG, i.e. the number of dependencies  
    * Space complexity: O(M)  

  Why orthogonal list?  
    * If adjacency list is used to represent the DAG, the time complexity would be O(M*S), where M is
      the number of arcs in the DAG and S is the number of starting tasks.