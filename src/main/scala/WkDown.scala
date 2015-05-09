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
    val uc = u.openConnection().asInstanceOf[HttpURLConnection]

    val contentType: String = uc.getContentType
    val encodingStart: Int = contentType.indexOf("charset=")
    if (encodingStart != -1) encoding = contentType.substring(encodingStart + 8)

    // TODO:: exception handle
    val s = new BufferedSource(uc.getInputStream)(encoding)
    val r = s.getLines()

    val rs = r.collect {
      case ACCEPT_REGX(article, size) => Some(article, size.toLong)
      case _ => None
    }.filter(_ != None).map(_.get).toList
    s.close()
    uc.disconnect()
    rs
  }

  def downloadStream(article_name: String, destdir: String): Boolean = {
    var bis: Option[BufferedInputStream] = None
    var bos: Option[BufferedOutputStream] = None
    var uc: Option[HttpURLConnection] = None
    try {
      val u = new URL(url + article_name)
      uc = Some(u.openConnection().asInstanceOf[HttpURLConnection])
//      uc.get.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
//      uc.get.setRequestProperty("Accept-Encoding", "gzip, deflate, sdch")
//      uc.get.setRequestProperty("Accept-Language", "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4")
//      uc.get.setRequestProperty("Connection", "keep-alive")
//      uc.get.setRequestProperty("Host", "dumps.wikimedia.org")
//      uc.get.setRequestProperty("If-Range", "Mon, 06 Apr 2015 15:42:20 GMT")
//      uc.get.setRequestProperty("Range", "bytes=1152-1152")
//      uc.get.setRequestProperty("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/41.0.2272.76 Chrome/41.0.2272.76 Safari/537.36")
//
//      uc.get.connect()

      bis = Some(new BufferedInputStream(uc.get.getInputStream))
      bos = Some(new BufferedOutputStream(new FileOutputStream(destdir+article_name)))

      val buffer = new Array[Byte](BUFFER_SIZE)
      var bytes = bis.get.read(buffer)
      while ( bytes != -1) {
        bos.get.write(buffer, 0, bytes)
        bos.get.flush()
        bytes = bis.get.read(buffer)
      }

      uc.get.getHeaderField("Content-Type") == "application/octet-stream"
    } catch {
      case e: Exception =>
        logInfo("failed download article " + article_name + " by exception: " + e)
        false
    } finally {
      if (bis == null)
        bis.get.close()
      if (bos.isDefined)
        bos.get.close()
      if (uc.isDefined)
        uc.get.disconnect()
    }

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

    if (files.length > 1) {
      files.foreach(fn => hdfs.copyFromLocalFile(false, true, new Path(fn), new Path(hdfswiki+fn.substring(tmpdir_data.length))))
    } else {
      files.foreach(fn => hdfs.copyFromLocalFile(false, true, new Path(fn), new Path(hdfswiki+fn.substring(tmpdir_data.length, fn.length-11))))
    }


    logInfo("article " + article_name + " moved to hdfs /wikidown/" + article_name)

    val end = System.currentTimeMillis()
    end-start
  }

}

object WkDown {
  var wd: Option[WkDown] = None
  def apply(): WkDown = wd.isDefined match {
    case true => wd.get
    case false =>
      wd = Some(new WkDown)
      wd.get
  }
}