package MapReduce

import Generation.RSGStateMachine.unit
import HelperUtils.Parameters
import com.mifmif.common.regex.Generex
import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, Text}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.PrivateMethodTester
import MapReduce.LogAnalyzer
import collection.JavaConverters._

import language.deprecated.symbolLiterals
import org.scalatest.matchers.should.Matchers

class MapReduceTest extends AnyFlatSpec with Matchers {
  behavior of "Map Reduce functions"

  it should "Get the correct time interval from log time" in {
    val logTimestamp1 = "18:13:25.657"
    val actual1 = LogAnalyzer.hashTimeString(logTimestamp1)
    actual1 shouldBe "18:13:2-29"
    val logTimestamp2 = "18:13:22.657"
    val actual2 = LogAnalyzer.hashTimeString(logTimestamp2)
    actual1 shouldBe "18:13:2-29"
  }

  it should "Correctly identify strings with regex pattern" in {
    val containsPattern = "8G;,3m_T`G#H]&Yh:Ei1%fp''5`I7fcg1ae3X5rS9hM5rQ7hce2cf0bf0Z6fJ6wbe2bf0ag0%<Bqz8fMm#{JWqMdoc_2N/|wf8]"
    val doesNotContainPattern = "k1H+R#/%d0>Q>N:6\\enT]U"
    LogAnalyzer.containsInjectionPattern(containsPattern) shouldBe true
    LogAnalyzer.containsInjectionPattern(doesNotContainPattern) shouldBe false
  }

  it should "Correctly find the max value in list" in {
    val values = List(1, 2, 3, 4, 5, 4, 3, 2, 1, 0).map(new IntWritable(_)).asJava
    val expected = new IntWritable(5)
    LogAnalyzer.findMax(values) shouldBe expected
  }

  it should "Correctly sum values in a list" in {
    val values = List(1, 2, 3, 4, 5, 4, 3, 2, 1, 0).map(new IntWritable(_)).asJava
    val expected = new IntWritable(25)
    LogAnalyzer.sumValues(values) shouldBe expected
  }

  it should "Correctly select reducer method for character count key" in {
    val values = List(1, 2, 3, 4, 5, 4, 3, 2, 1, 0).map(new IntWritable(_)).asJava
    val key = new Text("DEBUG_Characters")
    LogAnalyzer.reduceValuesCorrectly(key, values) shouldBe new IntWritable(5)
  }

  it should "Correctly reduce values for occurrences key" in {
    val values = List(1, 2, 3, 4, 5, 4, 3, 2, 1, 0).map(new IntWritable(_)).asJava
    val key = new Text("ERROR_Total_Generated")
    LogAnalyzer.reduceValuesCorrectly(key, values) shouldBe new IntWritable(25)
  }

  it should "Correctly reduce values for time interval key" in {
    val values = List(1, 2, 3, 4, 5, 4, 3, 2, 1, 0).map(new IntWritable(_)).asJava
    val key = new Text("18:13:26_INFO_No_Injection")
    LogAnalyzer.reduceValuesCorrectly(key, values) shouldBe new IntWritable(25)
  }
}