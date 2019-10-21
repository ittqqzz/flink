package com.tqz.java.ECommerceRecommendSystem.recomender

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS


case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

// 定义标准推荐对象
case class Recommendation(productId: Int, score: Double)

// 定义用户的推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

case class Rating(userId: Int, productId: Int, score: Double)

object OfflineRecommender {

  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val PATH = "C:\\Users\\tqz\\IdeaProjects\\flink\\src\\main\\resources\\ratings.csv"
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val csv = env.readCsvFile[Rating](PATH)
//    csv.print()

    // 提取出所有用户和商品的数据集
    val userId = csv.map(_.userId).distinct()
    val productId = csv.map(_.productId).distinct()

    val trainData = csv.map(x => Rating(x.userId, x.productId, x.score))

    // 设定ALS学习器
    val als = ALS()
      .setIterations(10)
      .setNumFactors(10)
      .setBlocks(100)

    // 通过一个map参数设置其他参数
    val parameters = ParameterMap()
      .add(ALS.Lambda, 0.01)
      .add(ALS.Seed, 42L)

    val res = als.predict(trainData, parameters)

    res.print()

    env.execute("OfflineRecommender")
  }

}
