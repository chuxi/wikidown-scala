import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by king on 15-5-8.
 */

// learn how to use hdfs
object HDFSHelper {
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://10.214.208.11:9000")

  lazy val hdfs = FileSystem.get(conf)

  def main(args: Array[String]): Unit = {

    // get file list
//    val it = hdfs.listFiles(new Path("/wikidown"), false)
//    val ab = new ArrayBuffer[String]()
//    while (it.hasNext) {
//      ab += it.next().getPath.getName
//    }
//    ab.foreach(println)

    hdfs.mkdirs(new Path("/texts"))

    // copy local to remote hdfs
    hdfs.copyFromLocalFile(false, true,
      Array(new Path(getClass.getResource("/hello.txt").toString),
        new Path(getClass.getResource("/hello3.txt").toString),
        new Path(getClass.getResource("/hello2.txt").toString)), new Path("/texts"))


//    hdfs.concat(new Path("/texts/text"), Array(new Path("/texts/hello2.txt"),new Path("/texts/hello3.txt")))

    hdfs.delete(new Path("/texts"), true)

    val files = Array("/hello2.txt", "/hello3.txt")



//    hdfs.rename(new Path("/wikidown/enwiki-latest-pages-articles21.xml-p013325003p015724999-part-00000"),
//                new Path("/wikidown/enwiki-latest-pages-articles21.xml-p013325003p015724999"))

//    hdfs.delete(new Path("/wikidown/enwiki-latest-pages-articles27.xml-p029625017p045581259"), true)
//    hdfs.delete(new Path("/wikidown/enwiki-latest-pages-articles27.xml-p029625017p046315516"), true)

  }



}
