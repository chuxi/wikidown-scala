__author__ = 'king'


import urllib, re
import os
import sys
import time
from random import randint

# how to import pyspark ...
os.environ['SPARK_HOME']="/usr/local/spark"
sys.path.append("/usr/local/spark/python")
sys.path.append("/usr/local/spark/python/build")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

# spilt the bz2 file
START_LINE = "  <page>\n"
END_LINE = "  </page>\n"
MAX_PAGE_IN_A_FILE = 1000000

# check downloaded files
def get_articles_downloaded(storpath):
    if not os.path.exists(storpath):
        os.mkdir(storpath)
        return []
    os.listdir(storpath)

def fix_articles_to_retrive(store_path, articles_to_retrive):
    articles_downloaded = get_articles_downloaded(store_path)
    for article in articles_downloaded:
        if (article in articles_to_retrive):
            articles_to_retrive.pop(article, None)
    return articles_to_retrive

# if failed, make sure it would try again
def download_articles(article_name, tmpfile):
    for i in range(0, 3):
        if os.path.exists(tmpfile):
            os.remove(tmpfile)
        (filename, headers) = urllib.urlretrieve("http://dumps.wikimedia.org/enwiki/latest/" + article_name, tmpfile)
        # a little different from the tutorial
        if headers["Content-Type"] == 'application/octet-stream':
            break
        if i == 2:
            raise Exception("Can not retrive article: " + article_name)
        time.sleep(randint(300, 500))

def get_url_lines(wikiurl):
    page = urllib.urlopen(wikiurl)
    lines = page.readlines()
    # print len(lines)
    return lines

def get_articles_to_retrive():
    wikiurl = "http://dumps.wikimedia.org/enwiki/latest/"
    lines = get_url_lines(wikiurl)
    # lines = ['<a href="enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2">enwiki-latest-pages-articles1.xml-p000000010p00..&gt;</a> 06-Apr-2015 14:59            46529467']
    # get the links to download, format enwiki-latest-pages-articles[0-9].xml-*.bz2
    ACCEPT_REGX = '<a href="(enwiki-latest-pages-articles[0-9]+.xml-[0-9p]+.bz2)".*</a>.* (\d+)'

    articles_to_retrive = {}
    for line in lines:
        m = re.search(ACCEPT_REGX, line)
        if m is not None:
            articles_to_retrive[m.group(1)] = {"size" : int(m.group(2))/1024/1024}
    return articles_to_retrive

def split_bz2_file(tmpfile):
    import bz2
    bzfile = bz2.BZ2File(tmpfile, 'rb')
    current_part = 0
    records = 0
    outfile = None
    found_match = False
    # too large file, read one by one line
    line = bzfile.readline()
    while line:
        if not found_match:
            if line == START_LINE:
                found_match = True
                if outfile is None:
                    outfile = open("%s-part-%05d" %(tmpfile, current_part), "wb")
                    current_part += 1
                outfile.write(line.strip() + " ")
        else:
            if (line != END_LINE):
                outfile.write(line.strip() + " ")
            else:
                outfile.write(line)
                found_match = False
                records += 1
                if records >= MAX_PAGE_IN_A_FILE:
                    records = 0
                    outfile.close()
                    outfile = None
        line = bzfile.readline()
    if records > 0:
        outfile.close()

    return current_part

def retrive_and_write_to_store_path(article_name):
    import shutil
    start = time.time()
    tmpfile = "/tmp/" + article_name

    print "start to download %s ..." % article_name
    download_articles(article_name, tmpfile)
    print "finished downloading %s" % article_name

    print "start to split file %s " % tmpfile
    num_parts = split_bz2_file(tmpfile)
    print "finished spliting file %s" % tmpfile

    # move the file parts into new store place
    # TODO: move files to HDFS
    if not os.path.exists("/tmp/data"):
        os.mkdir("/tmp/data")
    for part in range(0, num_parts):
        desfile = "/tmp/data/" + article_name + "-part-%05d" % part
        srcfile = "%s-part-%05d" %(tmpfile, part)
        if os.path.exists(desfile):
            os.remove(desfile)
        shutil.move(srcfile, desfile)
    print "file %s moved to /tmp/data/" % tmpfile

    end = time.time()
    return end-start

# print the index and partition content
def f(index, iter):
    out = [index]
    for item in iter:
        out.append(item)
    yield out

def f2(iter):
    out = []
    for article in iter:
        out.append([article, retrive_and_write_to_store_path(article)])
    yield out


if __name__ == "__main__":
    articles_to_retrive = get_articles_to_retrive()

    # normal method
    # for article_name in articles_to_retrive:
    #     retrive_and_write_to_store_path(article_name)

    # download files by spark
    sc = SparkContext("spark://node1:7077", "retrive wiki articles")
    # sc = SparkContext("local", "retrive wiki articles")
    articles_rdd = sc.parallelize(articles_to_retrive.keys(), 4)
    #
    partitions = articles_rdd.mapPartitionsWithIndex(f).collect()
    print partitions
    #
    fetched_articles = articles_rdd.mapPartitions(f2).collect()
    print fetched_articles

    sc.stop()
