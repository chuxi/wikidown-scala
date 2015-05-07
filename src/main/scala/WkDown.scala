import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by king on 15-5-6.
 */
object WkDown extends Logging{

  def get_articles_to_retrive() = {
    val wd = WikiDown()
    wd.get_articles_to_retrive()
  }

  def retrive_and_put_articles_to_hdfs(article_name: String): Long = {
    val wd = WikiDown()
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://10.214.208.11:9000")
    val hdfs = FileSystem.get(conf)

    val start = System.currentTimeMillis()

    logInfo("Start to download article " + article_name)
    wd.download_article(article_name, "/tmp/download_articles/")
    logInfo("Finished downloading article " + article_name)

    // decompress bz2 files and extract page info to a new file
    logInfo("Start to split article " + article_name)
    val num_parts = wd.split_bz2_to_file(article_name, "/tmp/download_articles/", "/tmp/download_articles/data/")
    logInfo("Finished spliting article " + article_name)

    val files = Range(0, num_parts).map(i => new Path("/tmp/download_articles/data/"+article_name+f"-part-$i%05d")).toArray
    hdfs.copyFromLocalFile(false, false, files, new Path("/wikidown/"+article_name.substring(0, article_name.length-4)))
    logInfo("article " + article_name + " moved to hdfs /wikidown/" + article_name)

    val end = System.currentTimeMillis()
    end-start
  }

  def f(index: Int, iter: Iterator[String]) = {
    val ab = ArrayBuffer[String]()
    ab += index.toString
    iter.map(ab += _)
  }

  def f2(iter: Iterator[String]) = {
    iter.map(article => (article, retrive_and_put_articles_to_hdfs(article)))
  }


  def main(args: Array[String]) = {
    //    wd.download_article("enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2", "/tmp/download_articles/")

//    retrive_and_put_articles_to_hdfs("enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2")
    val sc = new SparkContext("spark://node1:7077", "wiki spark downloader")

    val articles_to_retrive = get_articles_to_retrive()

    articles_to_retrive.foreach(m => println(m._1 + "size: " + m._2/1024/1024))

    val partitions = sc.parallelize(get_articles_to_retrive(), 4).map(_._1)

    val partitionsWithIndex = partitions.mapPartitionsWithIndex(f).collect()

    partitionsWithIndex.foreach(println)

    val rs = partitions.mapPartitions(f2).collect()

    rs.foreach(println)

    sc.stop()
  }

}
