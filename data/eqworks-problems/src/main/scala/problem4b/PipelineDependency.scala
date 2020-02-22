/*
  Problem 4b. PipelineDependency

  Introduction:
    Data structure: doubly-linked orthogonal list
    Algorithm: depth first search (DFS)
    Time complexity: O(M), where M is the amount of arcs in the DAG, i.e. the amount of dependencies
    Space complexity: O(M)

    Why orthogonal list?
      If adjacency list is used to represent the DAG, the time complexity would be O(M*S), where M is
      the amount of arcs in the DAG and S is the amount of starting tasks.
*/

package problem4b

import java.util

import org.apache.spark.sql.SparkSession

import scala.collection.{JavaConverters, mutable}

object PipelineDependency {

//  val QUESTION_PATH: String = "C:\\Users\\wuwei\\Desktop\\IdeaProjects\\eqworks-problems\\data\\question.txt"
//  val RELATIONS_PATH: String = "C:\\Users\\wuwei\\Desktop\\IdeaProjects\\eqworks-problems\\data\\relations.txt"
//  val TASKS_PATH: String = "C:\\Users\\wuwei\\Desktop\\IdeaProjects\\eqworks-problems\\data\\task_ids.txt"
//  val SCHEDULE_PATH: String = "C:\\Users\\wuwei\\Desktop\\IdeaProjects\\eqworks-problems\\data\\schedule"
  val TAR_PATH: String = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
  val QUESTION_PATH: String = TAR_PATH + "/question.txt"
  val RELATIONS_PATH: String = TAR_PATH + "/relations.txt"
  val TASKS_PATH: String = TAR_PATH + "/task_ids.txt"
  val SCHEDULE_PATH: String = TAR_PATH + "/schedule"

  def solution(spark: SparkSession): List[Int] = {

    println("******************** Problem 4b. Pipeline Dependency begins ********************")

    // load tasks
    val tasks: Set[Int] = this.getTasks(spark)

    // load starting tasks
    val startingTasks: List[Int] = getStartingTasks(spark, tasks)

    // load the goal task
    val goalTask: Int = getGoalTask(spark, tasks)
    if(goalTask == -1) {
      println("Terminated: illegal goal task!")
      return List()
    }

    // load dependencies
    val relations: List[(Int, Int)] = getRelations(spark, tasks)

    // construct the DAG
    val nodeMap: Map[Int, Node] = constructGraph(tasks, relations)

    // remove the finished tasks
    removeFinishedTasks(startingTasks, nodeMap)

    // get the schedule of the goal task
    val schedule: List[Int] = getGoalTaskSchedule(goalTask, nodeMap)

    // save the results
    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(schedule).toDF.repartition(1).write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .save(SCHEDULE_PATH)

    println("******************** Problem 4b. Pipeline Dependency results ********************")
    schedule.foreach(println)

    schedule
  }

  private def getTasks(spark: SparkSession): Set[Int] = {
    spark.sparkContext
      .textFile(TASKS_PATH)
      .flatMap(line => line.trim.split(","))
      .map(_.toInt)
      .collect
      .toSet
  }

  private def getStartingTasks(spark: SparkSession, tasks: Set[Int]): List[Int] = {
    val line: String = spark.sparkContext
                .textFile(QUESTION_PATH)
                .first
                .split(":")(1)
                .trim

    // For the sake of robustness, the starting task list can be successfully read
    // if the task ids are either separated by "," or " ".
    var startingTask: List[Int] = null
    if(line.contains(" ")) {
      startingTask = line.split(" ").map(_.toInt).filter(tasks.contains).toList
    } else {
      startingTask = line.split(",").map(_.toInt).filter(tasks.contains).toList
    }
    startingTask.filter(tasks.contains)
  }

  private def getGoalTask(spark: SparkSession, tasks: Set[Int]): Int = {
    val lines: Array[String] = spark.sparkContext.textFile(QUESTION_PATH).collect
    var task: Int = -1

    // return -1 if the task id cannot be successfully read
    try{
      task = lines(1).split(":")(1).trim.toInt
    } catch {
      case e: NumberFormatException => task = -1
    }

    // return -1 if the task id is not contained in the task set
    if(!tasks.contains(task)) {task = -1}
    task
  }

  private def getRelations(spark: SparkSession, tasks: Set[Int]): List[(Int, Int)] = {
    spark.sparkContext
      .textFile(RELATIONS_PATH)
      .map{line =>
        val twoTasks = line.trim.split("->")
        (twoTasks(0).toInt, twoTasks(1).toInt)
      }
      .filter(x => tasks.contains(x._1) && tasks.contains(x._2))
      .collect
      .toList
  }

  private case class Node(val taskId: Int, var in: Arc, var out: Arc)

  private case class Arc(val tail: Int, val head: Int, var preIn: Arc, var nextIn: Arc,
                         var preOut: Arc, var nextOut: Arc)

  private def constructGraph(tasks: Set[Int], relations: List[(Int, Int)]): Map[Int, Node] = {
    // build nodes
    var nodeMap: Map[Int, Node] = tasks.map(x => (x, Node(x, null, null))).toMap

    // build arcs
    var inArcMap: Map[Int, mutable.Set[Arc]] = tasks.map(x => (x, mutable.Set[Arc]())).toMap
    var outArcMap: Map[Int, mutable.Set[Arc]] = tasks.map(x => (x, mutable.Set[Arc]())).toMap

    for(relation <- relations) {
      val curTask: Int = relation._1
      val nextTask: Int = relation._2
      var arc = Arc(curTask, nextTask, null, null, null, null)
      outArcMap.apply(curTask) += arc
      inArcMap.apply(nextTask) += arc
    }

    // collect the nodes and the arcs accordingly
    for(taskId <- nodeMap.keys) {
      val node = nodeMap.apply(taskId)
      node.in = connectInArcs(inArcMap.apply(taskId))
      node.out = connectOutArcs(outArcMap.apply(taskId))
    }

    nodeMap
  }

  private def connectInArcs(inArcSet: mutable.Set[Arc]): Arc = {
    var firstArc: Arc = null
    var preArc: Arc = null

    for(arc <- inArcSet) {
      if(firstArc == null) {firstArc = arc}
      else {
        preArc.nextIn = arc
        arc.preIn = preArc
      }
      preArc = arc
    }
    firstArc
  }

  private def connectOutArcs(outArcSet: mutable.Set[Arc]): Arc = {
    var firstArc: Arc = null
    var preArc: Arc = null

    for(arc <- outArcSet) {
      if(firstArc == null) {firstArc = arc}
      else {
        preArc.nextOut = arc
        arc.preOut = preArc
      }
      preArc = arc
    }
    firstArc
  }

  private def removeSubTree(task: Int, nodeMap: Map[Int, Node], removedTaskList: util.ArrayList[Int]): Unit = {
    val node: Node = nodeMap.apply(task)

    while(node.in != null) {
      val preTask: Int = node.in.tail
      removeSubTree(preTask, nodeMap, removedTaskList)
    }

    removeLeafNode(task, nodeMap)
    if(removedTaskList != null) {removedTaskList.add(task)}
  }

  private def removeLeafNode(task: Int, nodeMap: Map[Int, Node]): Unit = {
    val node: Node = nodeMap.apply(task)
    var outArcListHead = node.out

    while(outArcListHead != null) {
      val preInArc = outArcListHead.preIn
      val nextInArc = outArcListHead.nextIn
      if(nextInArc != null) {nextInArc.preIn = preInArc}
      if(preInArc == null) {nodeMap.apply(outArcListHead.head).in = nextInArc}
      else {preInArc.nextIn = nextInArc}
      outArcListHead = outArcListHead.nextOut
    }
  }

  private def removeFinishedTasks(startingTasks: List[Int], nodeMap: Map[Int, Node]): Unit = {
    for(task <- startingTasks) {
      val node: Node = nodeMap.apply(task)
      while(node.in != null) {
        val preTask: Int = node.in.tail
        removeSubTree(preTask, nodeMap, null)
      }
    }
  }

  private def getGoalTaskSchedule(goalTask: Int, nodeMap: Map[Int, Node]): List[Int] = {
    val schedule = new util.ArrayList[Int]
    removeSubTree(goalTask, nodeMap, schedule)
    JavaConverters.asScalaIteratorConverter(schedule.iterator).asScala.toSeq.toList
  }
}
