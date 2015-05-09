import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by king on 15-5-9.
 */
object WikiDown {

  def get_articles_to_retrive() = {
    val wd = new WkDown()
    val hdfs = {
      val conf = new Configuration()
      conf.set("fs.defaultFS", "hdfs://10.214.208.11:9000")
      FileSystem.get(conf)
    }

    val all_articles = wd.get_articles_from_html()
    val wp = new Path(wd.hdfswiki)
    if (!hdfs.exists(wp))
      hdfs.mkdirs(wp)

    val it = hdfs.listFiles(wp, false)
    val ab = new ArrayBuffer[String]()
    while (it.hasNext) {
      ab += it.next().getPath.getName
    }

    all_articles.filter(n => !ab.contains(n._1.substring(0, n._1.length-4)))
  }

  def f(index: Int, iter: Iterator[String]) = {
    iter.map(index.toString + " " + _)
  }

  def f2(iter: Iterator[String]) = {
    iter.map(article => (article, new WkDown().retrive_and_put_articles_to_hdfs(article)))
  }

  def main(args: Array[String]) = {
    //    wd.download_article("enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2", "/tmp/download_articles/")

    //    retrive_and_put_articles_to_hdfs("enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2")
    val conf = new SparkConf().setAppName("wiki spark downloader").setMaster("spark://node1:7077")
    val sc = new SparkContext(conf)

    val articles_to_retrive = get_articles_to_retrive()

    articles_to_retrive.foreach(m => println(m._1 + "  size: " + m._2/1024/1024))

    val partitions = sc.parallelize(articles_to_retrive, 4).map(_._1)

    val partitionsWithIndex = partitions.mapPartitionsWithIndex(f).collect()

    partitionsWithIndex.foreach(println)

    val rs = partitions.mapPartitions(f2).collect()

    rs.foreach(println)

    sc.stop()
  }

}
