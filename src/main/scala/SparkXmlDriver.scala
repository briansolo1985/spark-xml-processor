import java.io.ByteArrayInputStream
import javax.xml.parsers.{DocumentBuilderFactory, SAXParserFactory}
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}

import org.apache.commons.lang3.time.StopWatch
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

object SparkXmlDriver extends Logging{

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("spark-xml")

    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<book")
    jobConf.set("stream.recordreader.end", "</book>")
    FileInputFormat.addInputPaths(jobConf, "/var/tmp/input/*")

    val sparkContext = new SparkContext(sparkConf)

    val watch = new StopWatch

    logWarning("Building rawRDD...")
    val rawXmls = sparkContext.hadoopRDD(
      jobConf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[Text],
      classOf[Text])
    .map(xml => xml.toString.substring(1, xml.toString.length - 2))
    .cache
    logWarning("rawRDD is ready")

    logWarning("JSON parser")
    watch.reset
    watch.start
    rawXmls
      .map(xml => org.json.XML.toJSONObject(xml.toString))
      .map(_.getJSONObject("book").getString("author"))
      .saveAsTextFile("/var/tmp/output/json-xml")
    watch.stop
    logWarning("JSON: " + watch.getTime)
    logWarning("Memory usage: " + sparkContext.getExecutorMemoryStatus)

    logWarning("DOM parser")
    watch.reset
    watch.start
    rawXmls
      .map(xml => {
        val db = DocumentBuilderFactory.newInstance.newDocumentBuilder()
        val document = db.parse(new ByteArrayInputStream(xml.getBytes))
        document.getElementsByTagName("author").item(0).getTextContent
      })
      .saveAsTextFile("/var/tmp/output/dom-xml")
    watch.stop
    logWarning("DOM: " + watch.getTime)

    logWarning("SAX parser")
    watch.reset
    watch.start
    rawXmls
      .map(xml => {
        val parser = SAXParserFactory.newInstance.newSAXParser
        val handler = new SaxHandler
        parser.parse(new ByteArrayInputStream(xml.getBytes), handler)
        handler.getAuthor
      })
      .saveAsTextFile("/var/tmp/output/sax-xml")
    watch.stop
    logWarning("SAX: " + watch.getTime)

    logWarning("STAX parser")
    watch.reset
    watch.start
    rawXmls
      .map(xml => {
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(xml.getBytes))

        def parse(reader: XMLStreamReader): String = {
          var i = 0
          while(reader.hasNext) {
            val event = reader.next
            if (event == XMLStreamConstants.START_ELEMENT) {
              if("author".equals(reader.getLocalName)) {
                i = 1
              }
            } else if(event == XMLStreamConstants.CHARACTERS && i == 1) {
              return reader.getText.trim
            }
          }
          "NA"
        }
        parse(reader)
      })
      .saveAsTextFile("/var/tmp/output/stax-xml")
    watch.stop
    logWarning("STAX: " + watch.getTime)

    rawXmls.unpersist()

//    val sqlContext = new SQLContext(sparkContext)
//
//    logWarning("SPARK-XML parser")
//    watch.reset
//    watch.start
//    val df = sqlContext.read
//      .format("org.apache.spark.sql.xml")
//      .option("rootTag", "book")
//      .load("/var/tmp/input/*")
//      .select("author")
//      .write
//      .parquet("/var/tmp/output/spark-xml")
//    watch.stop
//    logWarning("SPARK-XML: " + watch.getTime)

  }
}
