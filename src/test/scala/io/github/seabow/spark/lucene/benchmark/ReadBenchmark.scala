package io.github.seabow.spark.lucene.benchmark

import io.github.seabow.spark.lucene.benchmark.utils.PerformanceUtil
import org.apache.spark.sql.functions.{col, explode_outer}
import org.scalameter.api._

object ReadBenchmark extends Bench.OfflineRegressionReport {
  val numRecords = Gen.range("numRecords")(200000, 1000000, 200000)

  private val readOpts = Context(
    exec.minWarmupRuns -> 1,
    exec.maxWarmupRuns -> 1,
    exec.benchRuns -> 2,
    exec.independentSamples -> 1,
    exec.jvmflags -> List("-Xms4g", "-Xmx4g")
  )

  def void(numRecord:Int):Unit={}
  private val beforeReading={
    numRecord =>
      void(numRecord) // 在基准测试之前先生成随机文件
  }

  // 准备阶段
  def generateRandom(numRecords: Int): Unit = {
    println(s"generating random $numRecords")
    // 在这里编写生成随机文件的逻辑
    PerformanceUtil.generateRandom(numRecords)
  }

  /**
   *  Deserialization Benchmark for lucene and other formats
   */
  performance of "Deserialize" config readOpts  in {
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num =>
//          PerformanceUtil.read(num, "orc").foreach{row=>}
      }
    }
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num =>
//          PerformanceUtil.read(num, "lucene").foreach{row=>}
      }
    }
  }

  /**
   * Array Contains Count Filter test
   */
  val arrContainsCondition="array_contains(map_tags.`sports`,'basketball')"
  performance of "ArrayContainsCount" config readOpts  in {
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num => PerformanceUtil.read(num, "orc").filter(arrContainsCondition).count()
      }
    }
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num => PerformanceUtil.read(num, "lucene").filter(arrContainsCondition).count()
      }
    }
  }

  /**
   * Map exists Count Filter test
   */
  val mapExistsCondition="map_tags['sports'] is not null"
  performance of "MapExistsCount" config readOpts  in {
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num => PerformanceUtil.read(num, "orc").filter(mapExistsCondition).count()
      }
    }
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num => PerformanceUtil.read(num, "lucene").filter(mapExistsCondition).count()
      }
    }
  }

  /**
   * Facet Count test
   */

  performance of "FacetCount" config readOpts  in {
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num => PerformanceUtil.read(num, "orc"
        ).selectExpr("map_tags.`art` as art"
        ).withColumn("art",explode_outer(col("art"))).groupBy("art").count().collect()
      }
    }
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num => PerformanceUtil.read(num, "lucene",Map("enforceFacetSchema"->"true")
        ).groupBy("map_tags.`art`").count().collect()
      }
    }
  }

  /**
   * Filtered Facet Count test
   */

  performance of "ArrayContainsFacetCount" config readOpts  in {
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num => PerformanceUtil.read(num, "orc"
        ).filter(mapExistsCondition).selectExpr("map_tags.`art` as art"
        ).withColumn("art",explode_outer(col("art"))).groupBy("art").count().collect()
      }
    }
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num => PerformanceUtil.read(num, "lucene",Map("enforceFacetSchema"->"true")
        ).filter(mapExistsCondition).groupBy("map_tags.`art`").count().collect()
      }
    }
  }

}
