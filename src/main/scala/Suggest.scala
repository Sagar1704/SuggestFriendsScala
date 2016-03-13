package main.scala

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.util.Sorting

object Suggest {
    val DELIMITER = '%'
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("suggest");
        val sc = new SparkContext(conf);

        val input = sc.textFile(args(0))
        val profiles = input.map(profile => profile.split("\t")).filter(profile => (profile.length == 2))
        val friendsRDD1 = profiles.map(profile => createDontRecommendMap(profile)).flatMap(reco => reco)
        val friendsRDD2 = profiles.map(profile => createRecommendMap(profile)).flatMap(reco => reco)

        val toBeRecommendedRDD = friendsRDD1.union(friendsRDD2)
        val pairsRDD = toBeRecommendedRDD.reduceByKey((pair1, pair2) => (pair1 + DELIMITER + pair2))
        val mutualFriendsCountRDD = pairsRDD.map(line => (line._1, countMutualFriends(line._2))).sortBy(_._2, false).sortBy(_._1.split("-")(0).toLong)
        //        val recommendationsRDD = mutualFriendsCountRDD.map(line => line._1).map(line => (line.split("-")(0) -> line.split("-")(1))).reduceByKey((x, y) => x + "," + y).sortBy(_._1.toLong)
        //        val recommendationsRDD = mutualFriendsCountRDD.map(line => (line._1.split("-")(0), (line._1.split("-")(1), line._2))).reduceByKey((pair1, pair2) => someMethod(pair1, pair2)).sortBy(_._1.toLong)//.map(pair => getRecommendations(pair))//.map(pair => (pair._1, getRecommendations(pair._2)))
        val recommendationsRDD = mutualFriendsCountRDD.map(line => (line._1.split("-")(0), (line._1.split("-")(1), "" + line._2))).reduceByKey((pair1, pair2) => (pair1._1 + "," + pair2._1, pair1._2 + "," + pair2._2)).sortBy(_._1.toLong).map(pair => getRecommendations(pair)) //.map(pair => (pair._1, getRecommendations(pair._2)))
        val output = recommendationsRDD.map(profile => (profile._1 + "\t" + profile._2))
        output.saveAsTextFile(".\\output")

    }

    def someMethod(pair1: (String, Int), pair2: (String, Int)): (String, Int) = {
        if (pair1._2 == 0 && pair2._2 == 0)
            return ("", 0)
        else {
            if (pair1._2 == 0)
                return (pair2._1, pair2._2)
            else if (pair2._2 == 0)
                return (pair1._1, pair1._2)
            else {
                if (pair1._2 == pair2._2) {
                    if (pair1._1.toLong < pair2._1.toLong)
                        return (pair1._1 + "," + pair2._1, pair1._2 + pair2._2)
                    else
                        return (pair2._1 + "," + pair1._1, pair1._2 + pair2._2)
                } else
                    return (pair1._1 + "," + pair2._1, pair1._2 + pair2._2)
            }
        }
    }

    def getRecommendations(pair: (String, (String, String))): (String, String) = {
        val user = pair._1
        val friends = pair._2._1.split(",")
        val mutualCounts = pair._2._2.split(",")

        var countFriendsMap = new LinkedHashMap[Int, ArrayBuffer[Long]]
        var index = 0
        while (index < friends.length) {
            val count = mutualCounts(index).toInt
            if (count != 0) {
                if (countFriendsMap.contains(count)) {
                    var array = countFriendsMap.get(count).get
                    array.append(friends(index).toLong)
                    val sortedArray = array.sortWith(_ < _)
                    countFriendsMap.put(count, sortedArray)
                } else {
                    var array = new ArrayBuffer[Long]
                    array.append(friends(index).toLong)
                    val sortedArray = array.sortWith(_ < _)
                    countFriendsMap.put(count, sortedArray)
                }
            }
            index = index + 1

        }

        val recommendations = countFriendsMap.flatMap(pair => pair._2).toList.mkString(",")

        return (user, recommendations)

    }

    def createDontRecommendMap(profile: Array[String]): HashMap[String, String] = {
        val user = profile(0)
        val friends = profile(1).split(",")
        val toBeRecommended = new HashMap[String, String]
        for (friend <- friends) {
            toBeRecommended.put(user + "-" + friend, "")
        }
        return toBeRecommended
    }

    def createRecommendMap(profile: Array[String]): HashMap[String, String] = {
        val user = profile(0)
        val friends = profile(1).split(",")
        val toBeRecommended = new HashMap[String, String]
        for (friend1 <- friends) {
            for (friend2 <- friends) {
                if (friend1 != friend2) {
                    toBeRecommended.put(friend1 + "-" + friend2, user)
                }
            }
        }
        return toBeRecommended
    }

    def countMutualFriends(friends: String): Int = {
        if (friends.trim().equals("") || friends.startsWith("" + DELIMITER)) {
            return 0
        } else {
            val delimiterCount = friends.count(_ == DELIMITER)
            val friendsCount = friends.length() - delimiterCount
            return friendsCount
        }
    }
}