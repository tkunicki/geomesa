/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.api.java;

import org.locationtech.geomesa.spark.GeoMesaSpark$;

import java.io.Serializable;
import java.util.Map;

public class JavaGeoMesaSpark {
    public static JavaSpatialRDDProvider apply(Map<String, Serializable> params) {
        return new JavaSpatialRDDProvider(GeoMesaSpark$.MODULE$.apply(params));
    }
}
