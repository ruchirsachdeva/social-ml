package com.twitter.spark

import com.twitter.spark.mllib.MLDataAnalysis
;

/**
  * Created by SBI on 1/26/2019.
  */
object FileDataAnalyzer {

  def main(args: Array[String]) {

    MLDataAnalysis.analyze("D:/tmp/tweets_out/json/2019/01/12/17")
  }
}
