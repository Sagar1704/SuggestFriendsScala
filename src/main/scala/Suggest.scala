package main.scala

import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

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
        val mutualFriendsCountRDD = pairsRDD.map(line => (line._1, countMutualFriends(line._2))).filter(line => (line._2 > 0)).sortBy(_._2, false)
        val recommendationsRDD = mutualFriendsCountRDD.map(line => line._1).map(line => (line.split("-")(0) -> line.split("-")(1))).reduceByKey((x, y) => x + "," + y).sortBy(_._1.toLong)

        val output = recommendationsRDD.map(profile => (profile._1 + "\t" + profile._2))
        output.saveAsTextFile(".\\output")

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
        val delimiterCount = friends.count(_ == DELIMITER)
        val friendsCount = friends.length() - friends.count(_ == DELIMITER)
        if (friendsCount - delimiterCount == 1)
            return friendsCount
        else
            return -1
    }
}