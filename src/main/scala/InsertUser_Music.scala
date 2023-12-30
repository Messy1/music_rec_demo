import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import java.util.UUID
import scala.util.Try

object InsertUser_Music {
  def insertFromUser_Music(): Unit =  {
    val spark = SparkSession.builder()
      .appName("rating->user_singer")
      //      .master("local")
      .getOrCreate()
    val data = spark.read
      .option("sep", "\t")
      .csv("hdfs://Master:9000/singer_rec_data/user_music.tsv")
      .toDF("username", "musicname", "singername", "play_cnt", "like")

    println("数据条数是：" + data.count())


    data.rdd.foreachPartition(p => {
      //获取HBase连接
      val hbaseConfig = HBaseConfiguration.create()
      hbaseConfig.set("hbase.zookeeper.quorum", "Master,Slave1")
      hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
      //根据自己集群设置如下一行配置值
      //hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
      //在IDE中设置此项为true，避免出现"hbase-default.xml"版本不匹配的运行时异常
      hbaseConfig.set("hbase.defaults.for.version.skip", "true")
      val hbaseConn = ConnectionFactory.createConnection(hbaseConfig)
      val resultTable = TableName.valueOf("user_music")
      //获取表连接
      val table = hbaseConn.getTable(resultTable)

      //      p.foreach(r => {
      //        val put = new Put(Bytes.toBytes(r.getString(0)+'\t'+r.getString(1)))
      //        put.addColumn(Bytes.toBytes(FAMILYCOLUMN), Bytes.toBytes("singername"), Bytes.toBytes(r.getString(0)))
      //        put.addColumn(Bytes.toBytes(FAMILYCOLUMN), Bytes.toBytes("musicname"), Bytes.toBytes(r.getString(1)))
      //
      //        Try(table.put(put)).getOrElse(table.close()) //将数据写入HBase，若出错关闭table
      //      })

      try {
        p.foreach(row => {
          if (row.getString(0) != null && row.getString(1) != null && row.getString(2) != null && row.getString(3) != null && row.getString(4) != null) {
            val put = new Put(Bytes.toBytes(row.getString(0) + '\t' + row.getString(1) + '\t' + row.getString(2)))
            put.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("user"), Bytes.toBytes(row.getString(0)))
            put.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("music"), Bytes.toBytes(row.getString(1)))
            put.addColumn(Bytes.toBytes("entity"), Bytes.toBytes("singer"), Bytes.toBytes(row.getString(2)))
            put.addColumn(Bytes.toBytes("interact"), Bytes.toBytes("play_cnt"), Bytes.toBytes(row.getString(3)))
            put.addColumn(Bytes.toBytes("interact"), Bytes.toBytes("like"), Bytes.toBytes(row.getString(4)))
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
