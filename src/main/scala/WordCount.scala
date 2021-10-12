import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.slf4j.Logger

import java.lang
import java.util.StringTokenizer
import java.util.regex.Pattern
import collection.JavaConverters.*

object WordCount :
  def hashTimeString(logTime : String) : String = {
    val times = logTime.split(":")
    val second = times(2).toDouble.round.toString
    s"${times(0)}:${times(1)}:$second"
  }
  val config = ObtainConfigReference("randomLogGenerator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()
    val logger : Logger = CreateLogger(classOf[TokenizerMapper])

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val log : Array[String] = value.toString.split(" ")
      val timeBucket = hashTimeString(log(0).toString)
      val messageType = log(2)
      val key = s"${messageType}_${timeBucket}"
      word.set(key)

      val patternRegex = config.getString("randomLogGenerator.Pattern")
      val pattern = Pattern.compile(patternRegex)
      logger.info(s"Mapping logs to pattern $patternRegex")

      if (pattern.matcher(value.toString).find())  {
          context.write(word, one)
      }
    }
  }

  class IntSumReader extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }


  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration, "word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    configuration.set("mapred.textoutputformat.separatorText", ",")
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }


