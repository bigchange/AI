package com.bigchange.test

import com.bigchange.datamining.Rating

/**
  * Created by C.J.YOU on 2016/9/8.
  */
object DistanceRecommendTest {

  def main(args: Array[String]) {

    // Map：包含每个用户对每个Item的评分list集合
    val user = Map (

      "Hailey" -> List(new Rating("Broken Bells",4.0),new Rating("Deadmau5",1.0),new Rating("Norah Jones",4.0),new Rating("The Strokes",4.0),new Rating("Vampire Weekend",1.0)),
      "Veronica" -> List(new Rating("Blues Traveler",3.0),new Rating("Norah Jones",5.0),new Rating("Phoenix",4.0),new Rating("Slightly Stoopid",2.5),new Rating("The Strokes",3.0)),
      "Angelica" -> List(new Rating("Blues Traveler", 3.5),new Rating("Broken Bells", 2.0), new Rating("Norah Jones", 4.5), new Rating("Phoenix", 5.0), new Rating("Slightly Stoopid", 1.5), new Rating("The Strokes", 2.5), new Rating("Vampire Weekend", 2.0)),
      "Bill" -> List(new Rating("Blues Traveler", 2.0), new Rating("Broken Bells", 3.5), new Rating("Deadmau5", 4.0), new Rating("Phoenix", 2.0), new Rating("Slightly Stoopid", 3.5), new Rating("Vampire Weekend", 3.0)),
      "Chan" -> List(new Rating("Blues Traveler", 5.0), new Rating("Broken Bells", 1.0), new Rating("Deadmau5", 1.0), new Rating("Norah Jones", 3.0), new Rating("Phoenix", 5), new Rating("Slightly Stoopid", 1.0)),
      "Dan" -> List(new Rating("Blues Traveler", 3.0), new Rating("Broken Bells", 4.0), new Rating("Deadmau5", 4.5), new Rating("Phoenix", 3.0), new Rating("Slightly Stoopid", 4.5), new Rating("The Strokes", 4.0), new Rating("Vampire Weekend", 2.0)),
      "Jordyn" -> List(new Rating("Broken Bells", 4.5), new Rating("Deadmau5", 4.0), new Rating("Norah Jones", 5.0), new Rating("Phoenix", 5.0), new Rating("Slightly Stoopid", 4.5), new Rating("The Strokes", 4.0), new Rating("Vampire Weekend", 4.0)),
      "Sam" -> List(new Rating("Blues Traveler", 5.0), new Rating("Broken Bells", 2.0), new Rating("Norah Jones", 3.0), new Rating("Phoenix", 5.0), new Rating("Slightly Stoopid", 4.0), new Rating("The Strokes", 5.0))

    )

    // 距离测试
    /*val dis = DistanceRecommend.manhattan(user("Hailey"),user("Jordyn"))
    println("dis:" + dis)

    val nearnest = DistanceRecommend.computeNearestNeighbor("Hailey", user)
    println("nearnest:" + nearnest)

    val res = DistanceRecommend.recommend("Hailey", user)
    println("res:" + res)*/

    // cosSim
    /*val v1 = new DoubleMatrix(Array(4.75, 4.5, 5.0, 4.25, 4.0))
    val v2 = new DoubleMatrix(Array(4.0, 3.0, 5.0, 2.0,1.0))
    val cosSim = DistanceRecommend.cosSim(v1,v2)
    println("cosSim:" + cosSim)*/


  }

}
