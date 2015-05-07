import java.io._
import java.net.{URLConnection, URL}


import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.conf.Configuration

import scala.io.{Source, BufferedSource}
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Created by king on 15-5-6.
 */

class WikiDown private() {

  val url = "http://dumps.wikimedia.org/enwiki/latest/"
  val BUFFER_SIZE = 2000
  val START_LINE = "  <page>"
  val END_LINE = "  </page>"
  val MAX_PAGE_IN_A_FILE = 1000000

  def r = {
    var encoding: String = "UTF-8"
    val u = new URL(url)
    val uc = u.openConnection()
    val contentType: String = uc.getContentType
    val encodingStart: Int = contentType.indexOf("charset=")
    if (encodingStart != -1) encoding = contentType.substring(encodingStart + 8)

    // TODO:: exception handle
    new BufferedSource(uc.getInputStream)(encoding).getLines()
  }


  def get_articles_to_retrive(): List[(String, Long)] = {
    val ACCEPT_REGX = """<a href="(enwiki-latest-pages-articles[0-9]+.xml-[0-9p]+.bz2)".*</a>.* (\d+)""".stripMargin.r
    r.collect {
      case ACCEPT_REGX(article, size) => Some(article, size.toLong)
      case _ => None
    }.filter(_ != None).map(_.get).toList
  }

  def download_article(article_name: String, destdir: String): Unit = {
    val d = new File(destdir)
    def downloadStream(article_name: String, destdir: String): Boolean = {
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
    }
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
            fout = Some(new PrintWriter(new BufferedOutputStream(new FileOutputStream(destdir+article_name+f"-part-$current_part%05d"))))
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

}


object WikiDown {
  var wd: Option[WikiDown] = None
  def apply(): WikiDown = {
    if (!wd.isDefined)
      wd = Some(new WikiDown())
    wd.get
  }
}
