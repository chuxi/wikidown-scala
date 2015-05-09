import java.io._
import java.net.{URLConnection, HttpURLConnection, URL}

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkConf, Logging, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.BufferedSource

/**
 * Created by king on 15-5-6.
 */

class WkDown extends Logging with Serializable {
  val url = "http://dumps.wikimedia.org/enwiki/latest/"
  val BUFFER_SIZE = 2000

  val START_LINE = "  <page>"
  val END_LINE = "  </page>"
  val MAX_PAGE_IN_A_FILE = 1000000

  val tmpdir = "/tmp/download_articles/"
  val tmpdir_data = "/tmp/download_articles/data/"
  val hdfswiki = "/wikidown/"

  def get_articles_from_html(): List[(String, Long)] = {
    val ACCEPT_REGX = """<a href="(enwiki-latest-pages-articles[0-9]+.xml-[0-9p]+.bz2)".*</a>.* (\d+)""".stripMargin.r

    var encoding: String = "UTF-8"
    val u = new URL(url)
    val uc = u.openConnection()
    val contentType: String = uc.getContentType
    val encodingStart: Int = contentType.indexOf("charset=")
    if (encodingStart != -1) encoding = contentType.substring(encodingStart + 8)

    // TODO:: exception handle
    val s = new BufferedSource(uc.getInputStream)(encoding)
    val r = s.getLines()
    s.close()

    r.collect {
      case ACCEPT_REGX(article, size) => Some(article, size.toLong)
      case _ => None
    }.filter(_ != None).map(_.get).toList
  }

  def downloadStream(article_name: String, destdir: String): Boolean = {
    //      try {
    val u = new URL(url + article_name)
    val uc = u.openConnection()
    val bis = new BufferedInputStream(uc.getInputStream)
    val bos = new BufferedOutputStream(new FileOutputStream(destdir+article_name))

    val buffer = new Array[Byte](BUFFER_SIZE)
    var bytes = bis.read(buffer)
    while ( bytes != -1) {
      bos.write(buffer, 0, bytes)
      bos.flush()
      bytes = bis.read(buffer)
    }
    bis.close()
    bos.close()

    uc.getHeaderField("Content-Type") == "application/octet-stream"
    //      } catch {
    //        case e: Exception =>
    //          logInfo("failed download article " + article_name + " by exception: " + e)
    //          false
    //      } finally {
    //        if (bis.isDefined)
    //          bis.get.close()
    //        if (bos.isDefined)
    //          bos.get.close()
    //      }

  }

  def download_article(article_name: String, destdir: String): Unit = {
    val d = new File(destdir)

    for ( i <- Range(0, 3)) {
      if (d.exists() && d.isDirectory)
        d.listFiles().filter(_.getName == article_name).foreach(_.delete())
      else
      if (d.exists() && !d.isDirectory || !d.exists())
        d.mkdir()
      if (downloadStream(article_name, destdir))
        return
      if (i == 2)
        throw new Exception("Can not download article " + article_name)
      Thread.sleep(300*1000)
    }
  }

  def split_bz2_to_file(article_name: String, sourdir: String, destdir: String): Int = {
    val d = new File(destdir)
    if (d.exists() && d.isDirectory)
      d.listFiles().filter(_.getName.startsWith(article_name)).foreach(_.delete())
    else
    if (d.exists() && !d.isDirectory || !d.exists())
      d.mkdir()

    val bzin = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(sourdir+article_name), false)))
    var current_part = 0
    var records = 0
    var fout = None: Option[PrintWriter]
    var found_match = false

    var line: String = bzin.readLine()
    while ( line != null) {
      if (! found_match) {
        if (line == START_LINE) {
          found_match = true
          if (! fout.isDefined) {
            fout = Some(new PrintWriter(new BufferedOutputStream(new FileOutputStream(destdir+article_name.substring(0, article_name.length-4)+f"-part-$current_part%05d"))))
            current_part += 1
          }
          fout.get.write(line.trim + " ")
        }
      } else {
        if (line != END_LINE)
          fout.get.write(line.trim + " ")
        else {
          fout.get.write(line.trim + "\n")
          found_match = false
          records += 1
          if (records >= MAX_PAGE_IN_A_FILE) {
            records = 0
            fout.get.close()
            fout = None
          }
        }
      }
      line = bzin.readLine()
    }
    if (records > 0)
      fout.get.close()

    bzin.close()
    current_part
  }



  def get_articles_to_retrive() = {
    val hdfs = {
      val conf = new Configuration()
      conf.set("fs.defaultFS", "hdfs://10.214.208.11:9000")
      FileSystem.get(conf)
    }

    val all_articles = get_articles_from_html()
    val wp = new Path(hdfswiki)
    if (!hdfs.exists(wp))
      hdfs.mkdirs(wp)

    val it = hdfs.listFiles(wp, false)
    val ab = new ArrayBuffer[String]()
    while (it.hasNext) {
      ab += it.next().getPath.getName
    }

    all_articles.filter(n => !ab.contains(n._1.substring(0, n._1.length-4)))
  }

  def retrive_and_put_articles_to_hdfs(article_name: String): Long = {
    val hdfs = {
      val conf = new Configuration()
      conf.set("fs.defaultFS", "hdfs://10.214.208.11:9000")
      FileSystem.get(conf)
    }

    val start = System.currentTimeMillis()

    logInfo("Start to download article " + article_name)
    download_article(article_name, tmpdir)
    logInfo("Finished downloading article " + article_name)

    // decompress bz2 files and extract page info to a new file
    logInfo("Start to split article " + article_name)
    val num_parts = split_bz2_to_file(article_name, tmpdir, tmpdir_data)
    logInfo("Finished spliting article " + article_name)

    val files = Range(0, num_parts).map(i => tmpdir_data+article_name.substring(0, article_name.length-4)+f"-part-$i%05d").toArray

    files.foreach(fn => hdfs.copyFromLocalFile(false, true, new Path(fn), new Path(hdfswiki+fn.substring(tmpdir_data.length))))

    logInfo("article " + article_name + " moved to hdfs /wikidown/" + article_name)

    val end = System.currentTimeMillis()
    end-start
  }

  def f(index: Int, iter: Iterator[String]) = {
    iter.map(index.toString + " " + _)
  }

  def f2(iter: Iterator[String]) = {
    iter.map(article => (article, retrive_and_put_articles_to_hdfs(article)))
  }

}

object WkDown {
  def main(args: Array[String]) = {
    val wd = new WkDown
    //    wd.download_article("enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2", "/tmp/download_articles/")

    //    retrive_and_put_articles_to_hdfs("enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2")
    val conf = new SparkConf().setAppName("wiki spark downloader").setMaster("spark://node1:7077")
    val sc = new SparkContext(conf)

    val articles_to_retrive = wd.get_articles_to_retrive()

    articles_to_retrive.foreach(m => println(m._1 + "  size: " + m._2/1024/1024))

    val partitions = sc.parallelize(articles_to_retrive, 4).map(_._1)

    val partitionsWithIndex = partitions.mapPartitionsWithIndex(wd.f).collect()

    partitionsWithIndex.foreach(println)

    val rs = partitions.mapPartitions(wd.f2).collect()

    rs.foreach(println)

    sc.stop()
  }
}
