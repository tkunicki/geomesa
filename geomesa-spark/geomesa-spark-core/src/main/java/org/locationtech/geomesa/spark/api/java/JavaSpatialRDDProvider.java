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
import org.apache.spark.rdd.RDD;
import org.geotools.data.Query;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.SpatialRDDProvider$;
import org.locationtech.geomesa.spark.SpatialRDDProvider.*;
import org.opengis.feature.simple.SimpleFeature;
import scala.Function1;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class JavaSpatialRDDProvider {

    @FunctionalInterface
    public interface JavaTransform<T>  {
        JavaRDD<T> apply(SpatialRDD spatialRDD);
    }

    public static <T> JavaTransform<T> wrap(Function1<SpatialRDD, RDD<T>> transform) {
        return in -> transform.apply(in).toJavaRDD();
    }

    public static final JavaTransform<SimpleFeature> SimpleFeatureTransform =
            wrap(SpatialRDDProvider$.MODULE$.toSimpleFeatureWithKryoRegistration());
    public static final JavaTransform<List<Object>> ValueListTransform =
            wrap(SpatialRDDProvider$.MODULE$.toValueList());
    public static final JavaTransform<List<Entry<String,Object>>> KeyValueListTransform =
            wrap(SpatialRDDProvider$.MODULE$.toKeyValueList());
    public static final JavaTransform<Map<String, Object>> MapTransform =
            wrap(SpatialRDDProvider$.MODULE$.toJavaMap());
    public static final JavaTransform<String> GeoJSONStringTransform =
            wrap(SpatialRDDProvider$.MODULE$.toGeoJSONString());
    public static final JavaTransform<List<Object>> PyValueListTransform =
            in -> {
                int i = in.schema().indexOf("geom");
                return ValueListTransform.apply(in).map(l -> { l.set(i, l.get(i).toString()); return l;} );
            };
    public static final JavaTransform<List<Object[]>> PyKeyValueListTransform =
            in -> {
                final int i = in.schema().indexOf("geom");
                return KeyValueListTransform.apply(in).
                        map(l -> l.stream().
                                map(e -> new Object[] { e.getKey(), e.getValue()}).
                                collect(Collectors.toList())).
                        map(l -> { l.get(i)[1] = l.get(i)[1].toString(); return l; });
            };
    public static final JavaTransform<Map<String, Object>> PyMapTransform =
            in -> MapTransform.apply(in).map(m -> { m.put("geom", m.get("geom").toString()); return m;} );

    private final SpatialRDDProvider wrapped;

    JavaSpatialRDDProvider(SpatialRDDProvider wrapped) {
        this.wrapped = wrapped;
    }

    public boolean canProcess(Map<String, Serializable> params) {
        return wrapped.canProcess(params);
    }

    public JavaRDD<SimpleFeature> rdd(Configuration conf, JavaSparkContext jsc, Map<String, String> params, Query query) {
        return rdd(conf, jsc, params, query, SimpleFeatureTransform);
    }

    public <T> JavaRDD<T> rdd(Configuration conf, JavaSparkContext jsc, Map<String, String> params, Query query, JavaTransform<T> transform) {
        return transform.apply(wrapped.read(conf, jsc.sc(), toScalaMap(params), query));
    }

    public void save(JavaRDD<SimpleFeature> jrdd, Map<String, String> params, String typeName) {
        wrapped.save(jrdd.rdd(), toScalaMap(params), typeName);
    }

    static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> map) {
        return scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala().toMap(scala.Predef.conforms());
    }
}
