package alaaomar
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
object SparkRDDAPIs {
  def main(args: Array[String]): Unit = {

    // code segment used to prevent excessive logging
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val conf = new SparkConf().
      setMaster("local").
      setAppName("SparkAPIs")

    // creating spark context
    val sc = new SparkContext(conf)
    //1.	Read a text file composing of a large number of lines.
    // The content of this file will be stored as an RDD object (rddLines),
    // each line as a separate record in that RDD.
    val rddLines = sc.textFile("bigfile.txt")
    //2.	use flatMap Api to transform (rddLines) into a new RDD object (rddWords)
    // where each record corresponds to a single word.
    // Be aware that we need to exclude those words whose lengths are less than 2 characters.
    val rddWords = rddLines.flatMap(line=>line.split(" ")).filter( x => x.length() > 2)
    //3.	Find the frequency of each word by creating a new RDD object (rddWordFreq).
    // Then, store the results in a new text file called (txtWordFreq.txt)
    // where each record (word, freq) in the rdd is stored as a separate line using
    // the following format: <word> has occurred <freq> times
    val rddWordFrequency= rddWords.map(word=>(word,1)).reduceByKey(_+_)

    rddWordFrequency.map(x=>"<" + x._1 + "> has occurred <" + x._2 +">" ).saveAsTextFile("txtWordFreq.txt")

    //b.	sort the words based on their frequencies in descending order and print
    // the top 20 results.
    println(" Top 2o frequent words in descending order: ")
    rddWordFrequency.collect().sortBy(- _._2).take(20).foreach(println)
    //c.find and print the number of words ending with ‘ion’ and the number of words
    // ending with ‘ing’
    val rdding =rddWordFrequency.filter(x => x._1.endsWith("ing"))
    println(" Words ends with ing: ")
    rdding.collect().foreach(println)
    val rddion = rddWordFrequency.filter(x => x._1.endsWith("ion"))
    println(" Words ends with ion: ")
    rddion.collect().foreach(println)
   // d.	Find and print the number of unique words
    val rddunique = rddWordFrequency.filter(x => x._2 == 1)
    val unique_words = rddunique.count()
    println(" Number of unique words: ")
    println(unique_words)
    rddunique.collect().foreach(println)
    val words_frequencies_summation= rddWordFrequency.map(x=>x._2).sum()
    println("Word Frequencies")
    print(words_frequencies_summation)
    val rddAvg= rddWordFrequency.mapValues(x=> x/ words_frequencies_summation)
    println("Word Frequencies Average is:")
    rddAvg.collect().take(10).foreach(println)
    // Standard deviation
    val n= rddWordFrequency.collect().length // number of words
    val mean = words_frequencies_summation /n  // Average
    val rddVariance = rddWordFrequency.mapValues(x => (x - mean) * (x - mean) /(n-1))
    val rddStandDeviation = rddVariance.mapValues(x=>math.sqrt(x))
    println("The standard deviation is:")
    rddStandDeviation.take(10).foreach(println)

 }
}
