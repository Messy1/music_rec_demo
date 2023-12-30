import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import java.util.UUID
import scala.util.Try

object InsertSingers {
  def insertFromSingers(): Unit =  {
    val spark = SparkSession.builder()
      .appName("singers->singers")
      //.master("local")
      .getOrCreate()

    //读取singers.tsv
    val data = spark.read
      .option("sep", "\t")
      .csv("hdfs://Master:9000/singer_rec_data/singers.tsv")
      .toDF("singername")
    println("数据条数是：" + data.count())

    val FAMILYCOLUMN = "singer_info"
    data.rdd.foreachPartition(p => {
      //获取HBase连接
      val hbaseConfig = HBaseConfiguration.create()
      hbaseConfig.set("hbase.zookeeper.quorum", "Master,Slave1")
      hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
      //在IDE中设置此项为true，避免出现"hbase-default.xml"版本不匹配的运行时异常
      hbaseConfig.set("hbase.defaults.for.version.skip", "true")
      val hbaseConn = ConnectionFactory.createConnection(hbaseConfig)
      val resultTable = TableName.valueOf("singers")
      //获取表连接
      val table = hbaseConn.getTable(resultTable)
      try {
        p.foreach(r => {
          if (r.getString(0) != null) {
            val put = new Put(Bytes.toBytes(r.getString(0)))
            put.addColumn(Bytes.toBytes(FAMILYCOLUMN), Bytes.toBytes("singername"), Bytes.toBytes(r.getString(0)))
            Try(table.put(put)).recover {
              case e: Exception => e.printStackTrace()
            }
          }
        })
      } finally {
        // 关闭资源
        Try(table.close()).recover { case e: Exception => e.printStackTrace() }
        Try(hbaseConn.close()).recover { case e: Exception => e.printStackTrace() }
      }

      table.close()
      hbaseConn.close()
    })
  }
}
