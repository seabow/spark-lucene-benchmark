package io.github.seabow.spark.lucene.benchmark

import io.github.seabow.spark.lucene.benchmark.utils.PerformanceUtil
import io.github.seabow.spark.v2.lucene.LuceneOptions
import org.apache.spark.sql.functions._
import org.scalameter.api._

object FinalReportSuite extends Bench.OfflineRegressionReport{
  val numRecords = Gen.range("numRecords")(200000, 1000000, 200000)
  private val writeOpts = Context(
    exec.minWarmupRuns -> 1,
    exec.maxWarmupRuns -> 1,
    exec.benchRuns -> 2,
    exec.independentSamples -> 1,
    exec.jvmflags -> List("-Xms2g", "-Xmx2g")
  )

  @transient lazy private val beforeWriting={
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




  private val readOpts = Context(
    exec.minWarmupRuns -> 1,
    exec.maxWarmupRuns -> 1,
    exec.benchRuns -> 2,
    exec.independentSamples -> 1,
    exec.jvmflags -> List("-Xms4g", "-Xmx4g")
  )

  def void(numRecord:Int):Unit={}
 @transient lazy private val beforeReading={
    numRecord =>
      void(numRecord)
  }


  /**
   *  Deserialization Benchmark for lucene and other formats
   */
  performance of "Deserialize" config readOpts  in {
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num =>
          PerformanceUtil.read(num, "lucene",Map(LuceneOptions.vectorizedReadCapacity->"4096")).foreach{ row=>}
      }
    }
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num =>
          PerformanceUtil.read(num, "orc").foreach{row=>}
      }
    }
  }

  /**
   * Array Contains Count Filter test
   */
  val arrContainsCondition="array_contains(map_tags.`sports`,'basketball')"
  performance of "ArrayContainsCount" config readOpts  in {
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num => PerformanceUtil.read(num, "lucene").filter(arrContainsCondition).count()
      }
    }
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num => PerformanceUtil.read(num, "orc").filter(arrContainsCondition).count()
      }
    }
  }

  /**
   * Map exists Count Filter test
   */
  val mapExistsCondition="map_tags['sports'] is not null"
  performance of "MapExistsCount" config readOpts  in {
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num => PerformanceUtil.read(num, "lucene").filter(mapExistsCondition).count()
      }
    }
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num => PerformanceUtil.read(num, "orc").filter(mapExistsCondition).count()
      }
    }
  }

  /**
   * Facet Count test
   */

  performance of "FacetCount" config readOpts  in {
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num => PerformanceUtil.read(num, "lucene"
        ).select(explode_outer(col("map_tags.`art`")).as("art")).groupBy("art").count().collect()
      }
    }
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num => PerformanceUtil.read(num, "orc"
        ).select(explode_outer(col("map_tags.`art`")).as("art")).groupBy("art").count().collect()
      }
    }
  }

  /**
   * Filtered Facet Count test
   */

  performance of "ArrayContainsFacetCount" config readOpts  in {
    measure method "lucene" in {
      using(numRecords) setUp beforeReading in {
        num => PerformanceUtil.read(num, "lucene"
        ).filter(mapExistsCondition
        ).select(explode_outer(col("map_tags.`art`")).as("art")).groupBy("art").count().collect()
      }
    }
    measure method "orc" in {
      using(numRecords) setUp beforeReading  in {
        num => PerformanceUtil.read(num, "orc"
        ).filter(mapExistsCondition
        ).select(explode_outer(col("map_tags.`art`")).as("art")).groupBy("art").count().collect()
      }
    }
  }

}
