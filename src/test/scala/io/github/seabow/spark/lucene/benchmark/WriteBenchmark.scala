package io.github.seabow.spark.lucene.benchmark

import io.github.seabow.spark.lucene.benchmark.utils.PerformanceUtil
import org.scalameter.api._


object WriteBenchmark extends Bench.OfflineRegressionReport {
  val numRecords = Gen.range("numRecords")(200000, 1000000, 200000)

  private val writeOpts = Context(
    exec.minWarmupRuns -> 1,
    exec.maxWarmupRuns -> 1,
    exec.benchRuns -> 2,
    exec.independentSamples -> 1,
    exec.jvmflags -> List("-Xms2g", "-Xmx2g")
  )

  private val beforeWriting={
    numRecord =>
      generateRandom(numRecord) // 在基准测试之前先生成随机文件
  }

  // 准备阶段
  def generateRandom(numRecords: Int): Unit = {
    println(s"generating random $numRecords")
    // 在这里编写生成随机文件的逻辑
    PerformanceUtil.generateRandom(numRecords)
  }

  performance of "Writing test" config writeOpts  in {
    measure method "write lucene" in {
      using(numRecords) setUp beforeWriting in {
        num => PerformanceUtil.write(num, "lucene") // 执行基准测试
      }
    }
    measure method "write orc" in {
      using(numRecords) setUp beforeWriting  in {
        num => PerformanceUtil.write(num, "orc") // 执行基准测试
      }
    }
  }
}