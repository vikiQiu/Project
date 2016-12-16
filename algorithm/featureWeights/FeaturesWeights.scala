package io.transwarp.discover.statistic.weights

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseMatrix => BDM}
import breeze.numerics.abs
import io.transwarp.discover.statistic.weights.FeaturesWeights.TestMethods.TestMethods
import io.transwarp.hubble.error.{HubbleError, HubbleErrors}
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.mllib.stat.Statistics._
import org.apache.spark.mllib.stat.test.ChiSqTest

import scala.collection.mutable


/**
  * Created by endy on 16-12-6.
  */
object FeaturesWeights extends Logging{

  object TestMethods extends Enumeration {
    type TestMethods = Value
    val chiSq = Value("chiSq")
    val informationGain = Value("informationGain")
    val informationGainRatio = Value("informationGainRatio")
    val gini = Value("gini")
  }


  def computeWeights(matrixs: Array[Matrix], method: TestMethods): Array[Double] = {

    val results = new Array[Double](matrixs.length)

    matrixs.zipWithIndex.foreach{ case (matrix: Matrix, index: Int) =>
      method match {
        case TestMethods.chiSq =>
          results(index) = chisqCompute(matrix)
        case TestMethods.informationGain =>
          results(index) = informationGainCompute(matrix)
        case TestMethods.informationGainRatio =>
          results(index) = informationGainRatioCompute(matrix)
        case TestMethods.gini =>
          results(index) = giniGainCompute(matrix)
        //        case pca => pcaCompute(data)  // todo
        case _ =>
          throw HubbleErrors.typeNotSupported(s"${method} is not support")
      }
    }
    results
  }

  /** Method: Information Gain
    * compute information gain(D,a). y(category variable)
    * @param matrix: one element in the result of generateContingencyMatrix. Every row means different attribute of the feature a.
    *              Every column means different label of the response variable y.(y is a category feature.)
    * @return the information gain of the feature a: Double
    */
  def informationGainCompute(matrix: Matrix): Double = {

    val (colSums, rowSums, _ , numRows) =
      getColAndRowSums(matrix)

    var gain: Double = entropyCompute(colSums) // Compute the total(origin) entropy of D

    for (featureClassIndex <- 0 until numRows){
      gain -= rowSums(featureClassIndex) * entropyMatrixCompute(matrix, featureClassIndex, rowSums(featureClassIndex))/colSums.sum
    }

    gain
  }

  /** Method: Information Gain Ratio
    * compute the information gain ratio. y(category variable)
    * gain_ratio(D,a) = gain(D,a)/IV(a)
    * IV(a) = \sum_{v = 1}^V  \frac{|D^v|}{|D|} log_2  \frac{|D^v|}{|D|}
    * @param matrix
    * @return
    */
  def informationGainRatioCompute(matrix: Matrix): Double = {

    val (colSums, rowSums, _ , numRows) =
      getColAndRowSums(matrix)

    val totalCount = colSums.sum

    val gain = informationGainCompute(matrix)
    var iv = 0d // 0 is double
    for (featureClassIndex <- 0 until numRows) {
      val freq = rowSums(featureClassIndex)/ totalCount
      if (freq != 0) iv -= freq * log2(freq)
    }

    if (iv != 0) gain / iv else 0d
  }

  /** Method: Gini
    * compute gini gain of the attribute a
    * @param matrix the frequency matrix of attribute a and y(category variable)
    * @return the feature weight of attribute a: \text{Gini}(D) - \sum_{v = 1}^V  \frac{|D^v|}{|D|}\text{Gini}(D^v)
    */
  def giniGainCompute(matrix: Matrix): Double = {
    val (colSums , rowSums, _ , numRows) =
      getColAndRowSums(matrix)

    val total = rowSums.sum
    val giniTotal = giniCompute(colSums)

    var giniIndex:Double = 0
    for (featureClassIndex <- 0 until numRows){
      giniIndex += rowSums(featureClassIndex) * giniMatrixCompute(matrix, featureClassIndex, rowSums(featureClassIndex)) / total
    }
    giniTotal - giniIndex
  }

  /**
    * compute absolute correlation between attribute and response variable y(numeric variable)
    * @param data RDD[Vector] with all data
    * @param labelIndex the index of y
    * @return the absolute correlation
    */
  def correlationCompute(data: RDD[Vector], labelIndex: Int): Array[Double] = {
    val corMatrix = corr(data)
    val results = new Array[Double](data.first().size)
    for (i <- 0 until corMatrix.numCols) results(i) = abs(corMatrix(i,labelIndex))
    results
  }

  /**
    * compute absolute first principle component of attrubutes. y(Any)
    * @param attrs the RDD[Vector] of attributes (with no y)
    * @return
    */
  def pcaCompute(attrs: RDD[Vector]): Array[Double] = {
    val dataPca = new PCA(1)
    dataPca.fit(attrs).pc.toArray.map(ele => abs(ele))
  }

  /** Method: Chisq
    * Compute the chisquare statistics
    * @param matrix
    * @return 1 - pvalue(chisquare statistics)
    */
  def chisqCompute(matrix: Matrix): Double = 1 - ChiSqTest.chiSquaredMatrix(matrix).pValue//ChiSqTest.chiSquaredMatrix(matrix).statistic



  // **************** Assistant Functions: *****************
  /**
    * The contingency table is constructed from the raw (feature, label) pairs and used to conduct
    * the independence.
    * Returns an Array[Matrix]
    */
  def generateContingencyMatrix(data: RDD[ LabeledPoint]): Array[Matrix] = {
    val maxCategories = 10000
    val numCols = data.first().features.size
    val results = new Array[Matrix](numCols)
    var labels: Map[Double, Int] = null
    // at most 1000 columns at a time
    val batchSize = 1000
    var batch = 0
    while (batch * batchSize < numCols) {
      val startCol = batch * batchSize
      val endCol = startCol + math.min(batchSize, numCols - startCol)
      val pairCounts = data.mapPartitions { iter =>
        val distinctLabels = mutable.HashSet.empty[Double]
        val allDistinctFeatures: Map[Int, mutable.HashSet[Double]] =
          Map((startCol until endCol).map(col => (col, mutable.HashSet.empty[Double])): _*)
        var i = 1
        iter.flatMap { case LabeledPoint(label, features) =>
          if (i % 1000 == 0) {
            if (distinctLabels.size > maxCategories) {
              throw new HubbleError(HubbleErrors.InvalidColumn,
                s"The compute expect factors (categorical values) but found more" +
                  s" than $maxCategories distinct label values.", "")
            }
            allDistinctFeatures.foreach { case (col, distinctFeatures) =>
              if (distinctFeatures.size > maxCategories) {
                throw new HubbleError(HubbleErrors.InvalidColumn,
                  s"The compute expect factors (categorical values) but found more" +
                    s" than $maxCategories distinct label values.", "")
              }
            }
          }
          i += 1
          distinctLabels += label
          val brzFeatures = features.asBreeze
          (startCol until endCol).map { col =>
            val feature = brzFeatures(col)
            allDistinctFeatures(col) += feature
            (col, feature, label)
          }
        }
      }.countByValue()

      if (labels == null) {
        labels =
          pairCounts.keys.filter(_._1 == startCol).map(_._3).toArray.distinct.zipWithIndex.toMap
      }
      val numLabels = labels.size
      pairCounts.keys.groupBy(_._1).foreach { case (col, keys) =>
        val features = keys.map(_._2).toArray.distinct.zipWithIndex.toMap
        val numRows = features.size
        val contingency = new BDM(numRows, numLabels, new Array[Double](numRows * numLabels))
        keys.foreach { case (_, feature, label) =>
          val i = features(feature)
          val j = labels(label)
          contingency(i, j) += pairCounts((col, feature, label))
        }
        results(col) = Matrices.fromBreeze(contingency)
      }
      batch += 1
    }
    results
  }

  def printMatrix(matrix: Matrix): Unit ={
    for (i <- 0 until matrix.numRows){
      for (j <- 0 until matrix.numCols) print(matrix(i,j),"|")
      print('\n')
    }
    println("matrix done")
  }

  def log2(x: Double): Double = scala.math.log(x) / scala.math.log(2)

  def getColAndRowSums(matrix: Matrix): (Array[Double], Array[Double], Int, Int) = {
    val numRows = matrix.numRows
    val numCols = matrix.numCols
    val colSums = new Array[Double](numCols)
    val rowSums = new Array[Double](numRows)
    val colMajorArr = matrix.toArray
    val colMajorArrLen = colMajorArr.length

    // get row and column sums, col is label and row is feature
    var i = 0
    while (i < colMajorArrLen) {
      val elem = colMajorArr(i)
      if (elem < 0.0) {
        throw new HubbleError(HubbleErrors.InvalidColumn, "Contingency table cannot contain " +
          "negative entries.", "")
      }
      colSums(i / numRows) += elem
      rowSums(i % numRows) += elem
      i += 1
    }

    (colSums, rowSums, numCols, numRows)
  }

  /**
    * Compute the entropy of the row_th row
    * @param frequencyMatrix a frequency matrix
    * @param row the row to compute entropy
    * @param rowSum sum of the row_th row
    * @return the entropy of the row_th row
    */
  def entropyMatrixCompute(frequencyMatrix: Matrix, row: Int, rowSum: Double): Double = {
    var entropy:Double = 0
    for (labelClassIndex <- 0 until frequencyMatrix.numCols){
      val labelProb = frequencyMatrix(row, labelClassIndex) / rowSum
      if (labelProb != 0) entropy -= labelProb * log2(labelProb)
    }
    entropy
  }

  /**
    * Compute entropy of a vector: Entro
    * @param frequencyVector: a vector with frequency
    * @return the entropy of this vector
    */
  def entropyCompute(frequencyVector: Array[Double]): Double = {
    val frequencyMatrix = Matrices.dense(1,frequencyVector.length,frequencyVector)

    val totalCount = frequencyVector.sum
    entropyMatrixCompute(frequencyMatrix, 0, totalCount)
  }


  /**
    * compute gini index of a row in the matrix. For the less the gini index, the more purity the y, we use 1 - gini index
    * @param frequencyMatrix the matrix which row shows the label of attribute a and column show the label of response variable y
    * @param row the index of the row to compute gini index
    * @param rowSum row sum of the matrix
    * @return 1 - \text{Gini}(D)= 1 - \sum_{k=1}^{|y|} \sum_{k' \ne k} p_k p_{k'}=\sum_{k=1}^K \hat p_k (1-\hat p_k)
    */
  def giniMatrixCompute(frequencyMatrix: Matrix, row: Int, rowSum: Double): Double={
    var gini:Double = 0
    for (labelClassIndex <- 0 until frequencyMatrix.numCols){
      val prob = frequencyMatrix(row,labelClassIndex) / rowSum
      gini += prob*(1-prob)
    }
    gini
  }

  /**
    * similar to giniMatrixCompute, to compute gini index of the vector
    * @param frequencyVector the elements shows the frequency of the label of y
    * @return
    */
  def giniCompute(frequencyVector: Array[Double]): Double = {
    val frequencyMatrix = Matrices.dense(1,frequencyVector.length,frequencyVector)

    val totalCount = frequencyVector.sum
    giniMatrixCompute(frequencyMatrix, 0, totalCount)
  }

}
