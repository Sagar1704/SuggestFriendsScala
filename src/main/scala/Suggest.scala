package main.scala

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Suggest {
    val DELIMITER = '%'
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("suggest");
        val sc = new SparkContext(conf);

        val input = sc.textFile(args(0))

        val profiles = input.map(line => line.split("\t")).filter(line => (line.length == 2)).collect()
        var toBeRecommended = new ArrayBuffer[(String, String)]
        var index = 0
        while (index < profiles.length) {
            val user = profiles(index)(0)
            val friends = profiles(index)(1).split(",")
            val friendsRDD = sc.parallelize(friends)
            friendsRDD.map(friend => (user + "-" + friend, "")).foreach(pair => toBeRecommended.append(pair))
            for (friend1 <- friends) {
                for (friend2 <- friends) {
                    if (friend1 != friend2) {
                        toBeRecommended.append((friend1 + "-" + friend2, user))
                    }
                }
            }
            index = index + 1
        }

        for (pair <- toBeRecommended) {
            println(pair.toString())
        }

        val toBeRecommendedRDD = sc.parallelize(toBeRecommended)
        val pairsRDD = toBeRecommendedRDD.reduceByKey((pair1, pair2) => (pair1 + DELIMITER + pair2))
        val mutualFriendsCountRDD = pairsRDD.map(line => (line._1, countMutualFriends(line._2))).filter(line => (line._2 > 0)).sortBy(_._2, false)
        val recommendationsRDD = mutualFriendsCountRDD.map(line => line._1).map(line => (line.split("-")(0) -> line.split("-")(1))).groupBy(_._1).mapValues(_.map(_._2)).sortBy(_._1.toLong)
        
        recommendationsRDD.saveAsTextFile(".\\output")

    }

    def countMutualFriends(friends: String): Int = {
        val delimiterCount = friends.count(_ == DELIMITER)
        val friendsCount = friends.length() - friends.count(_ == DELIMITER)
        if (friendsCount - delimiterCount == 1)
            return friendsCount
        else
            return -1
    }
}