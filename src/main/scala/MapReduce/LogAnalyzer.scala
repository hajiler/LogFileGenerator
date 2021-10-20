package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.Logger

import collection.JavaConverters._
import java.lang
import java.util.regex.Pattern

object LogAnalyzer:
  // Create logger and access job configurations
  val logger: Logger = CreateLogger(classOf[LogMapper])
  val config = ObtainConfigReference("randomLogGenerator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  // Given the a log timestamp, return the 10 second time interval of where the log occurred.
  def hashTimeString(logTime: String): String = {
    val times = logTime.split(":")
    val second = (times(2).toDouble / 10).toInt.toString
    val interval = s"${times(0)}:${times(1)}:${second}-${second}9"
    logger.info(s"Mapping $logTime to interval: $interval")
    interval
  }
  // Check the log message for instance of the regex injection as designated in the compile
  def containsInjectionPattern(message: String): Boolean = {
    val patternRegex = config.getString("randomLogGenerator.Pattern")
    val pattern = Pattern.compile(patternRegex)
    logger.info(s"Checking for $patternRegex in message: ${message}")
    pattern.matcher(message).find()
  }

  // Given a parsed log, write corresponding computations.
  def checkForPatternAndMap(logType: String, timeBucket: String, message: String,
                            context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    // If pattern contains regex injection...
    if (containsInjectionPattern(message)) {
      // Write instance of 1, for this log type, for this particular time interval, containing regex.
      val key = new Text()
      key.set(s"${timeBucket}_${logType}_Injection")
      context.write(key, new IntWritable(1))
      // Write the number characters for this log type, as a possible maximum character number.
      key.set(s"Max_Characters_${logType}")
      context.write(key, new IntWritable(message.count(_ => true)))
    } else {
      // Write instance of 1, for this log type, for this particular time interval, NOT containing regex.
      val key = new Text()
      key.set(s"${timeBucket}_${logType}_No_Injection")
      context.write(key, new IntWritable(1))
    }
    // If the log type is of error, write an instance of the occurences to maintain track list of descending error time
    // intervals and the corresponding number of logs containing injection patterns.
    if(logType.equals("ERROR") && containsInjectionPattern(message)) {
      val key = new Text()
      key.set(s"ERROR_INJECTION_OCCURRENCES_DURING_${timeBucket}")
      context.write(key, new IntWritable(1))
    }
  }
  // Write instance of 1 message for the log type.
  def incrementNumLogType(logType: String, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val key = new Text()
    key.set(s"Total_Generated_${logType}")
    context.write(key, new IntWritable(1))
  }

  // Mapper Class, for mapping logs to various book keeping information.
  class LogMapper extends Mapper[Object, Text, Text, IntWritable] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      // Parse value for log time, log type, and actual message
      val log: Array[String] = value.toString.split(" ")
      val timeBucket = hashTimeString(log(0).toString)
      val logType = log(2)
      // Write book keeping information.
      checkForPatternAndMap(logType, timeBucket, value.toString, context)
      incrementNumLogType(logType, context)
    }
  }

  // Find maximum value given a list of IntWritables.
  def findMax(values: lang.Iterable[IntWritable]): IntWritable = {
    values.asScala.reduce((x, y) => if (x.get() > y.get()) x else y)
  }
  // Sum values from a list of IntWritables.
  def sumValues(values: lang.Iterable[IntWritable]): IntWritable = {
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    new IntWritable(sum)
  }

  // If the information key corresponding to character count, then find and return the maximum character account among
  // the possible values. Otherwise, the sum of values should be calculated.
  def reduceValuesCorrectly(key: Text, values: lang.Iterable[IntWritable]): IntWritable = {
    if (key.find("_Characters") > -1) {
      logger.info(s"Getting max value for key: $key")
      findMax(values)
    } else {
      logger.info(s"Summing values for key: $key")
      sumValues(values)
    }
  }

  // Reducer class for reducing book keeping information into unique key value pairs.
  class LogReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      context.write(key, reduceValuesCorrectly(key, values))
    }
  }

  // Main for configuring hadoop job, and Mapper/Reducers. Arg 0, is the input log file, Arg 1, is output directory path.
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration, "Analyzing Logs")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[LogMapper])
    job.setCombinerClass(classOf[LogReducer])
    job.setReducerClass(classOf[LogReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    configuration.set("mapred.textoutputformat.separatorText", ",")
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
