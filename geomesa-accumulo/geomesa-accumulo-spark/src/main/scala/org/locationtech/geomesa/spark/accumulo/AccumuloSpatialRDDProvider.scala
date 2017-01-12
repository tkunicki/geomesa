/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.accumulo

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.index.EmptyPlan
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.jobs.mapreduce._
import org.locationtech.geomesa.spark.SpatialRDDProvider
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.reflect._
import scala.util.Try

class AccumuloSpatialRDDProvider extends SpatialRDDProvider {
  import org.locationtech.geomesa.spark.CaseInsensitiveMapFix._
  import org.locationtech.geomesa.spark.TypeDefault._

  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(params)

  def inputFormat[T : ClassTag](): Class[InputFormat[Text, T]] = {
    (classTag[T] match {
      case c if c == classTag[SimpleFeature] => classOf[GeoMesaAccumuloInputFormat]
      case c if c == classTag[String] => classOf[GeoMesaAccumuloGeoJsonInputFormat]
      case c if c == classTag[Array[Byte]] => classOf[GeoMesaAccumuloKryoInputFormat]
      case c if c == classTag[Map[String, String]] => classOf[GeoMesaAccumuloMapInputFormat]
      case _ => throw new IllegalArgumentException
    }).asInstanceOf[Class[InputFormat[Text, T]]]
  }

  def rdd[T : ClassTag](conf: Configuration,
                        sc: SparkContext,
                        params: Map[String, String],
                        query: Query)(implicit default: T := SimpleFeature): RDD[T] = {
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    val username = AccumuloDataStoreParams.userParam.lookUp(params).toString
    val password = new PasswordToken(AccumuloDataStoreParams.passwordParam.lookUp(params).toString.getBytes)
    try {
      // get the query plan to set up the iterators, ranges, etc
      lazy val sft = ds.getSchema(query.getTypeName)
      lazy val qp = AccumuloJobUtils.getSingleQueryPlan(ds, query)

      if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
        sc.emptyRDD[T]
      } else {
        val instance = ds.connector.getInstance().getInstanceName
        val zookeepers = ds.connector.getInstance().getZooKeepers

        val transform = query.getHints.getTransformSchema

        ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, username, password)
        if (Try(params("useMock").toBoolean).getOrElse(false)){
          ConfiguratorBase.setMockInstance(classOf[AccumuloInputFormat], conf, instance)
        } else {
          ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], conf, instance, zookeepers)
        }
        InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], conf, qp.table)
        InputConfigurator.setRanges(classOf[AccumuloInputFormat], conf, qp.ranges)
        qp.iterators.foreach(InputConfigurator.addIterator(classOf[AccumuloInputFormat], conf, _))

        if (qp.columnFamilies.nonEmpty) {
          val cf = qp.columnFamilies.map(cf => new AccPair[Text, Text](cf, null))
          InputConfigurator.fetchColumns(classOf[AccumuloInputFormat], conf, cf)
        }

        InputConfigurator.setBatchScan(classOf[AccumuloInputFormat], conf, true)
        InputConfigurator.setBatchScan(classOf[GeoMesaAccumuloInputFormat], conf, true)
        GeoMesaConfigurator.setSerialization(conf)
        GeoMesaConfigurator.setTable(conf, qp.table)
        GeoMesaConfigurator.setDataStoreInParams(conf, params)
        GeoMesaConfigurator.setFeatureType(conf, sft.getTypeName)

        // set the secondary filter if it exists and is  not Filter.INCLUDE
        qp.filter.secondary
          .collect { case f if f != Filter.INCLUDE => f }
          .foreach { f => GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(f)) }

        transform.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))

        // Configure Auths from DS
        val auths = Option(AccumuloDataStoreParams.authsParam.lookUp(params).asInstanceOf[String])
        auths.foreach { a =>
          val authorizations = new Authorizations(a.split(","): _*)
          InputConfigurator.setScanAuthorizations(classOf[AccumuloInputFormat], conf, authorizations)
        }

        sc.newAPIHadoopRDD(conf, inputFormat[T], classOf[Text], classTag[T].runtimeClass.asInstanceOf[Class[T]]).map(U => U._2)
      }
    } finally {
      if (ds != null) {
        ds.dispose()
      }
    }
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param params
    * @param typeName
    */
  def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    try {
      require(ds.getSchema(typeName) != null,
        "Feature type must exist before calling save.  Call createSchema on the DataStore first.")
    } finally {
      ds.dispose()
    }

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
      val featureWriter = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
      try {
        iter.foreach { rawFeature =>
          FeatureUtils.copyToWriter(featureWriter, rawFeature, overrideFid = true)
          featureWriter.write()
        }
      } finally {
        IOUtils.closeQuietly(featureWriter)
        ds.dispose()
      }
    }
  }

}
