/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.api.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.geotools.data.Query;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.SpatialRDDProvider.*;
import org.opengis.feature.simple.SimpleFeature;

import java.io.Serializable;
import java.util.Map;

public class JavaSpatialRDDProvider {

    interface JavaFormat<T> {
        Format<T> wrapped();
    }

    private static class FormatWrapper<T> implements JavaFormat<T> {
        private final Format<T> wrapped;
        FormatWrapper(Format<T> wrapped) {
            this.wrapped = wrapped;
        }
        public Format<T> wrapped() { return wrapped; }
    }

    public static <T> FormatWrapper<T> wrap(Format<T> format) {
        return new FormatWrapper<T>(format);
    }

    public static final JavaFormat<SimpleFeature> SimpleFeatureFormat = wrap(SimpleFeatureFormat$.MODULE$);
    public static final JavaFormat<Map<String, Object>> JavaMapFormat = wrap(JavaMapFormat$.MODULE$);
    public static final JavaFormat<String> GeoJSONStringFormat = wrap(SpatialRDDProvider.GeoJSONStringFormat$.MODULE$);

    private final SpatialRDDProvider wrapped;

    JavaSpatialRDDProvider(SpatialRDDProvider wrapped) {
        this.wrapped = wrapped;
    }

    public boolean canProcess(Map<String, Serializable> params) {
        return wrapped.canProcess(params);
    }

    public JavaRDD<SimpleFeature> rdd(Configuration conf, JavaSparkContext jsc, Map<String, String> params, Query query) {
        return rdd(conf, jsc, params, query, SimpleFeatureFormat);
    }

    public <T> JavaRDD<T> rdd(Configuration conf, JavaSparkContext jsc, Map<String, String> params, Query query, JavaFormat<T> format) {
        return wrapped.rdd(conf,
                jsc.sc(),
                toScalaMap(params),
                query,
                format.wrapped()).toJavaRDD();
    }

    public void save(JavaRDD<SimpleFeature> jrdd, Map<String, String> params, String typeName) {
        wrapped.save(jrdd.rdd(), toScalaMap(params), typeName);
    }

    static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> map) {
        return scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala().toMap(scala.Predef.conforms());
    }
}
