package io.transwarp.discover.feature_weights

import io.transwarp.discover.statistic.weights.FeaturesWeights._
import io.transwarp.discover.statistic.weights.FeaturesWeights.TestMethods
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.linalg.{DenseVector, Matrix, Vectors,Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.TestingUtils._

/**
  * Created by viki on 16-12-8.
  */
class featureWeightSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {
//  val golfPwd =  "/home/viki/Documents/viki/data/golf01.csv"
  val golfPwd = "src/test/resources/testData/golf.csv"
  val polypwd = "src/test/resources/testData/polynominal.csv"
  val tolerance = 0.0001

  def getRddData(pwd: String, labelIndex: Int): RDD[LabeledPoint] = {
    // get RDD[LabeledPoint] data
    val distFile = sc.textFile(pwd)
    val arrs = distFile.map(_.split(","))
    val data = arrs.map{arr =>
      val vec = arr.toBuffer
      vec.remove(labelIndex)
      val label = arr(labelIndex).toDouble
      val vecs = vec.toArray.map(_.toDouble)
      new LabeledPoint(label, Vectors.dense(vecs))
    }
    data
  }

  def getMatrix(pwd: String, labelIndex: Int): Array[Matrix] = {
    val data = getRddData(pwd, labelIndex)
    val mat = generateContingencyMatrix(data)
    mat
  }

  def getAttributes(pwd: String, labelIndex: Int): RDD[Vector] = {
    // get RDD[LabeledPoint] data
    val distFile = sc.textFile(pwd)
    val arrs = distFile.map(_.split(","))
    val data = arrs.map{arr =>
      val vec = arr.toBuffer
      vec.remove(labelIndex)
      new DenseVector(vec.map(_.toDouble).toArray)
    }
    data.asInstanceOf[RDD[Vector]]
  }

  def getRddVector(pwd: String): RDD[Vector] = {
    // get RDD[LabeledPoint] data
    val distFile = sc.textFile(pwd)
    distFile.map(line => new DenseVector(line.split(",").map(_.toDouble))).asInstanceOf[RDD[Vector]]
  }

  def arrayTest(resPre: Array[Double], res: Array[Double]) = {
    var dist = 0.0
    for (i <- 0 until res.length) dist += (resPre(i) - res(i)).abs
    assert(dist ~== 0 absTol tolerance)
  }

  test("informationGain"){
    val mat = getMatrix(golfPwd,4)
    val resPre = computeWeights(mat, TestMethods.informationGain)
    arrayTest(resPre,Array(0.24674982, 0.79742882, 0.60065114,0.04812703))
  }

  test("informationGainRatio"){
    val mat = getMatrix(golfPwd,4)
    val resPre = computeWeights(mat,TestMethods.informationGainRatio)
    arrayTest(resPre,Array(0.15642756, 0.22643674, 0.18876494, 0.04884862))
  }

  test("gini"){
    val mat = getMatrix(golfPwd,4)
    val resPre = computeWeights(mat,TestMethods.gini)
    arrayTest(resPre,Array(0.11632653, 0.38775510, 0.29251701, 0.03061224))
  }

  test("chisquare"){
    val mat = getMatrix(golfPwd,4)
    val resPre = computeWeights(mat,TestMethods.chiSq)
    arrayTest(resPre,Array(0.8302338, 0.6228464, 0.5551705, 0.6660017))
  }

  test("PCA") {
    val data = getAttributes(polypwd,5)
    val resPre = pcaCompute(data.asInstanceOf[RDD[Vector]])
    arrayTest(resPre, Array(0.06516966, 0.41605649, 0.50428262, 0.41540871, 0.62911410))
  }

  test("correlation"){
    val data = getRddVector(polypwd)
    val resPre = correlationCompute(data.asInstanceOf[RDD[Vector]],5)
    arrayTest(resPre, Array(0.47329050, 0.65924921, 0.36146291, 0.04837433, 0.09039692))
  }

}
