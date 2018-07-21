package com.github.moonkev.spark

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.json4s.DefaultFormats


case class SampleOffset(offset: Long) extends org.apache.spark.sql.sources.v2.reader.streaming.Offset {

  implicit val defaultFormats: DefaultFormats = DefaultFormats
  override val json = offset.toString

  def +(increment: Long): SampleOffset = new SampleOffset(offset + increment)
  def -(decrement: Long): SampleOffset = new SampleOffset(offset - decrement)
}

object SampleOffset {

  def apply(offset: Offset): SampleOffset = new SampleOffset(offset.json.toLong)

  def convert(offset: Offset): Option[SampleOffset] = offset match {
    case sample: SampleOffset => Some(sample)
    case serialized: SerializedOffset => Some(SampleOffset(serialized))
    case _ => None
  }
}
