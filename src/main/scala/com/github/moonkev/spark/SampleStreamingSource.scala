package com.github.moonkev.spark

import java.util.Optional
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class SampleStreamingSource extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister {

  override def shortName(): String = "sample"

  override def createMicroBatchReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {
    println(s"SampleStreamingSource.createMicroBatchReader() Called - \n\tSchema = $schema")
    new SampleMicroBatchReader(options)
  }
}

class SampleMicroBatchReader(options: DataSourceOptions) extends MicroBatchReader with Logging {
  private val batchSize = options.get("batchSize").orElse("5").toInt
  private val producerRate = options.get("producerRate").orElse("5").toInt
  private val queueSize = options.get("queueSize").orElse("512").toInt

  private val NO_DATA_OFFSET = SampleOffset(-1)
  private var stopped: Boolean = false

  private var startOffset: SampleOffset = SampleOffset(-1)
  private var endOffset: SampleOffset = SampleOffset(-1)

  private var currentOffset: SampleOffset = SampleOffset(-1)
  private var lastReturnedOffset: SampleOffset = SampleOffset(-2)
  private var lastOffsetCommitted : SampleOffset = SampleOffset(-1)

  private val dataList: ListBuffer[Long] = new ListBuffer[Long]()
  private val dataQueue: BlockingQueue[Long] = new ArrayBlockingQueue(queueSize)

  private var producer: Thread = _
  private var consumer: Thread = _

  initialize()

  def initialize(): Unit = {

    producer = new Thread("Sample Producer") {
      setDaemon(true)
      override def run() {
        var counter: Long = 0
        while(!stopped) {
          dataQueue.put(counter)
          counter += 1
          Thread.sleep(producerRate)
        }
      }
    }
    producer.start()

    consumer = new Thread("Sample Producer") {
      setDaemon(true)
      override def run() {
        while (!stopped) {
          val id = dataQueue.poll(100, TimeUnit.MILLISECONDS)
          if (id != null.asInstanceOf[Long]) {
            dataList.append(id)
            currentOffset = currentOffset + 1
          }
        }
      }
    }
    consumer.start()
  }

  override def deserializeOffset(json: String): Offset = SampleOffset(json.toLong)

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    this.startOffset = start.orElse(NO_DATA_OFFSET).asInstanceOf[SampleOffset]
    this.endOffset = end.orElse(currentOffset).asInstanceOf[SampleOffset]
  }

  override def getStartOffset: Offset = {
    if (startOffset.offset == -1) {
      throw new IllegalStateException("startOffset is -1")
    }
    startOffset
  }

  override def getEndOffset: Offset = {
    if (endOffset.offset == -1) {
      currentOffset
    } else {
      if (lastReturnedOffset.offset < endOffset.offset) {
        lastReturnedOffset = endOffset
      }
      endOffset
    }
  }

  override def commit(end: Offset): Unit = {
    val newOffset = SampleOffset.convert(end).getOrElse(
      sys.error(s"SampleStreamMicroBatchReader.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )
    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt
    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }
    dataList.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    synchronized {
      val startOrdinal = startOffset.offset.toInt + 1
      val endOrdinal = endOffset.offset.toInt + 1

      val newBlocks = synchronized {
        val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
        val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
        assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
        dataList.slice(sliceStart, sliceEnd)
      }

      newBlocks.grouped(batchSize).map { block =>
        new SampleStreamBatchTask(block).asInstanceOf[DataReaderFactory[Row]]
      }.toList.asJava
    }
  }


  override def readSchema(): StructType = StructType(
    StructField("ID", LongType, nullable = true) :: StructField("Msg", StringType, nullable = true):: Nil)

  override def stop(): Unit = stopped = true
}

class SampleStreamBatchTask(dataList: ListBuffer[Long])
  extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new SampletreamBatchReader(dataList)
}

class SampletreamBatchReader(dataList: ListBuffer[Long]) extends DataReader[Row] {
  private var currentIdx = -1

  override def next(): Boolean = {
    currentIdx += 1
    currentIdx < dataList.size
  }

  override def get(): Row = Row(dataList(currentIdx), s"currentIdx = $currentIdx")

  override def close(): Unit = ()
}