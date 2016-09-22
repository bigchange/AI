package com.bigchange.test

import com.bigchange.datamining.{DistanceRecommend, Rating}

/**
  * Created by C.J.YOU on 2016/9/8.
  */
object DistanceRecommendTest {

  def main(args: Array[String]) {

    // Map：包含每个用户对每个Item的评分list集合
    val user = Map (

     /* "Clara" -> List(new Rating("Broken Bells",4.75),new Rating("Deadmau5",4.5),new Rating("Norah Jones",5),new Rating("The Strokes",4.25),new Rating("Vampire Weekend",4.0)),
      "Robert" -> List(new Rating("Broken Bells",4.0),new Rating("Deadmau5",3),new Rating("Norah Jones",5),new Rating("The Strokes",2.0),new Rating("Vampire Weekend",1.0)),
      "Hailey" -> List(new Rating("Broken Bells",4.0),new Rating("Deadmau5",1.0),new Rating("Norah Jones",4.0),new Rating("The Strokes",4.0),new Rating("Vampire Weekend",1.0)),
      "Veronica" -> List(new Rating("Blues Traveler",3.0),new Rating("Norah Jones",5.0),new Rating("Phoenix",4.0),new Rating("Slightly Stoopid",2.5),new Rating("The Strokes",3.0)),
      "Angelica" -> List(new Rating("Blues Traveler", 3.5),new Rating("Broken Bells", 2.0), new Rating("Norah Jones", 4.5), new Rating("Phoenix", 5.0), new Rating("Slightly Stoopid", 1.5), new Rating("The Strokes", 2.5), new Rating("Vampire Weekend", 2.0)),
      "Bill" -> List(new Rating("Blues Traveler", 2.0), new Rating("Broken Bells", 3.5), new Rating("Deadmau5", 4.0), new Rating("Phoenix", 2.0), new Rating("Slightly Stoopid", 3.5), new Rating("Vampire Weekend", 3.0)),
      "Chan" -> List(new Rating("Blues Traveler", 5.0), new Rating("Broken Bells", 1.0), new Rating("Deadmau5", 1.0), new Rating("Norah Jones", 3.0), new Rating("Phoenix", 5), new Rating("Slightly Stoopid", 1.0)),
      "Dan" -> List(new Rating("Blues Traveler", 3.0), new Rating("Broken Bells", 4.0), new Rating("Deadmau5", 4.5), new Rating("Phoenix", 3.0), new Rating("Slightly Stoopid", 4.5), new Rating("The Strokes", 4.0), new Rating("Vampire Weekend", 2.0)),
      "Jordyn" -> List(new Rating("Broken Bells", 4.5), new Rating("Deadmau5", 4.0), new Rating("Norah Jones", 5.0), new Rating("Phoenix", 5.0), new Rating("Slightly Stoopid", 4.5), new Rating("The Strokes", 4.0), new Rating("Vampire Weekend", 4.0)),
      "Sam" -> List(new Rating("Blues Traveler", 5.0), new Rating("Broken Bells", 2.0), new Rating("Norah Jones", 3.0), new Rating("Phoenix", 5.0), new Rating("Slightly Stoopid", 4.0), new Rating("The Strokes", 5.0)),
      // user for compute fixedCosSimilarity
      "David" -> List(new Rating("Imagine Dragons",3.0),new Rating("Daft Punk",5.0),new Rating("Lorde",4.0),new Rating("Fall Out Boy",1.0)),
      "Matt" -> List(new Rating("Imagine Dragons",3.0),new Rating("Daft Punk",4.0),new Rating("Lorde",4.0),new Rating("Fall Out Boy",1.0)),
      "Ben" -> List(new Rating("Kacey Musgraves",4.0),new Rating("Imagine Dragons",3.0),new Rating("Lorde",3.0),new Rating("Fall Out Boy",1.0)),
      "Chris" -> List(new Rating("Kacey Musgraves",4.0),new Rating("Imagine Dragons",4.0),new Rating("Daft Punk",4.0),new Rating("Lorde",3.0),new Rating("Fall Out Boy",1.0)),
      "Tori" -> List(new Rating("Kacey Musgraves",5.0),new Rating("Imagine Dragons",4.0),new Rating("Daft Punk",5.0),new Rating("Fall Out Boy",3.0)),
*/
    "Amy" -> List(new Rating("Taylor Swift",4.0),new Rating("PSY",3.0),new Rating("Whitney Houston",4.0)),
    "Ben" -> List(new Rating("Taylor Swift",5.0),new Rating("PSY",2.0)),
    "Clara" -> List(new Rating("PSY",3.5),new Rating("Whitney Houston",4.0)),
    "Daisy" -> List(new Rating("Taylor Swift",5.0),new Rating("Whitney Houston",3.0))
    )

    // 距离测试 @Test
    /*println("dis:" + DistanceRecommend.manhattan(user("Hailey"),user("Jordyn"))
    println("nearnest:" + DistanceRecommend.computeNearestNeighbor("Hailey", user))
    println("res:" + DistanceRecommend.recommend("Hailey", user))*/

    // cosSim @Test
    /*val v1 = new DoubleMatrix(Array(4.75, 4.5, 5.0, 4.25, 4.0))
    val v2 = new DoubleMatrix(Array(4.0, 3.0, 5.0, 2.0,1.0))
    val cosSim = DistanceRecommend.cosSim(v1,v2)
    println("cosSim:" + cosSim)*/

    //  person @Test
    /*println("person:"+ DistanceRecommend.pearson(user("Angelica"),user("Bill")))*/

    //  k最邻近算法 @Test
    /*println("kRecommend:" + DistanceRecommend.kRecommend("Angelica", user, 6))*/

    // 修正的余弦相似度 @Test
    /*println("fixedCosSim:" + DistanceRecommend.fixedCosSimilarity("Kacey Musgraves","Fall Out Boy",user))*/

    // 预测用户u对物品i的评分 @Test
   /* println("转换到评价体系后评分:" + DistanceRecommend.predictRating("David","Kacey Musgraves", user))*/

    // slope One @Test
    println("slope one:" + DistanceRecommend.slopeOneRecommendations("Ben","Whitney Houston",user))


  }

}
