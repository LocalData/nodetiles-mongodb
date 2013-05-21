Nodetiles-MongoDB
=============

Nodetiles-MongoDB is a data source for Nodetiles that allows you to load and render geographic data from a Mongo database

Quick Start
-----------

```
/* Set up the libraries */
var nodetiles = require('nodetiles-core');
var MongoDataSource = nodetiles.datasources.GeoJson;

/* Create your map context */
var map = new nodetiles.Map();

/* Add some data from Mongo! */
map.addData(new MongoDataSource({
  connectionString: "postgresql://localhost/database_name",
  tableName: "countries",
  geomField: "wkb_geometry",
  projection: "EPSG:900913"
}));
```

Copyright
---------
Copyright (c) 2012-2013 Code for America. See LICENSE for details.

