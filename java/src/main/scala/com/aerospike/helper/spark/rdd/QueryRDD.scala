package com.osscube.spark.aerospike.pdm

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.Filter

import com.aerospike.client.policy.{ClientPolicy, QueryPolicy}
import com.aerospike.client.cluster.Node
import com.aerospike.client.query.{Filter, RecordSet, Statement}
import com.aerospike.client.{AerospikeClient, Value}

case class AerospikePartition(index: Int,
                              endpoint: (String, Int, String),
                              startRange: Long,
                              endRange: Long
                               ) extends Partition()


class QueryRDD(@transient val sc: SparkContext,
               @transient val aerospikeHosts: Array[Node],
               val namespace: String,
               val set: String,
               val bins: Seq[String],
               val filterType: AeroFilterType,
               val filterBin: String,
               val filterStringVal: String) 
  extends RDD[Row](sc, Seq.empty) with Logging{
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    val stmt = new Statement()
    stmt.setNamespace(namespace)
    stmt.setSetName(set)
    stmt.setBinNames(bins: _*)
    
  }
}