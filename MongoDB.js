'use strict';
var mongoose = require('mongoose');
var projector = require("nodetiles-core").projector;
var __ = require("lodash");


/**
 * Turn stored a stored parcel result into geoJSON
 * TODO: Can save time and memory by not creating a new object here.
 * @param  {Object} item  A single response
 * @return {Object}       A single response structured as geoJSON
 */
function resultToGeoJSON(item, filter) {
  item.type = 'Feature';

  // Get the shape
  // Or if there isn't one, use the centroid.
  if (item.geo_info.geometry !== undefined) {
    item.id = item.parcel_id;
    item.geometry = item.geo_info.geometry;
  }else {
    item.id = item._id;
    item.geometry = {};
    item.geometry.type = 'Point';
    item.geometry.coordinates = item.geo_info.centroid;
  }

  item.properties = {
    geometry: __.cloneDeep(item.geo_info.geometry), // otherwise it gets projected
    name: item.geo_info.humanReadableName,
    id: item.id
  };

  // Clean up a bit
  delete item.geo_info.centroid;
  delete item.geo_info.geometry;
  delete item.responses;

  // If there is a filer, we also want the key easily accessible.
  if(filter) {
    // TODO: Handle the undefined condition
    if(item.hasOwnProperty("responses")) {
      if(item.responses.hasOwnProperty(filter.key)) {
        item.properties[filter.key] = item.responses[filter.key];
      }
      // else {
      //   obj.properties[filter.key] = 'undefined';
      // }
    }
    // else {
    //   obj.properties[filter.key] = 'undefined';
    // }
  }

  return item;
}

var MongoSource = function(options) {
  this._projection = projector.util.cleanProjString(options.projection || 'EPSG:4326');
  // Either options.db or options.connectionString is required.
  if (options.db) {
    this._db = options.db;
  } else {
    this._connectionString = options.connectionString;
    mongoose.connect(this._connectionString);
    this._db = mongoose.connection;
  }
  this._collectionName = options.collectionName;     // required
  this._geoKey = options.key;                        // required - name of the geodata field
  this._query =   options.query;                     // required - a query to get started with
  this._select = options.select;                     // required - fields to select
  this._filter = options.filter;                     // optional - key to filter data
  this.sourceName = options.name || 'localdata';
  return this;
};

MongoSource.prototype = {
  constructor: MongoSource,

  getShapes: function(minX, minY, maxX, maxY, mapProjection, callback) {

    // First, we need to turn the coordinates into a bounds query for Mongo
    var min = [minX, minY];
    var max = [maxX, maxY];

    // project request coordinates into data coordinates
    if (mapProjection !== this._projection) {
      min = projector.project.Point(mapProjection, this._projection, min);
      max = projector.project.Point(mapProjection, this._projection, max);
    }

    var query = this._query || {};
    var parsedBbox = [[min[0], min[1]], [max[0],  max[1]]];
    query[this._geoKey] = { '$within': { '$box': parsedBbox } };

    var start;

    function handleStream(stream) {
      var features = [];

      function addFeature(doc) {
        features.push(resultToGeoJSON(doc));
      }

      stream.on('data', addFeature);

      stream.on('close', function() {
        console.log("Fetched and processed " + features.length + " responses in " + (Date.now() - start) + "ms");

        callback(null, {
          type: 'FeatureCollection',
          features: features
        });
      });
    }

    var self = this;
    function handleCursor(cursor) {
      // The same thing as above, except using toArray
      // NB -- conversion to a format that the renderer wants is broken
      cursor.toArray(function(error, results) {
        if(error) {
          console.log("Error fetching results: ", error);
        }

        if(results) {
          console.log("Fetched " + results.length + " responses in " + (Date.now() - start) + "ms");
        }

        start = Date.now();
        var len = results.length;

        // Convert the results to geojson
        for (var i = 0; i < len; i++) {
          results[i] = resultToGeoJSON(results[i]);
        }

        var fc = {
          type: 'FeatureCollection',
          features: results
        };

        if (self._projection !== mapProjection) {
          fc = projector.project.FeatureCollection(self._projection, mapProjection, fc);
        }

        console.log("Processed " + results.length + " responses in " + (Date.now() - start) + "ms");

        callback(null, fc);

      }.bind(this));
    }

    // TODO: set autoreconnect
    //MongoClient.connect(this._connectionString, {
    //}, function(err, db) {
      //if(err) {
      //  console.log("Mongo error:", err);
      //  return;
      //}

      start = Date.now();

      // console.log("Query %j %j", this._query, this._select);

      // ----------------------------------------------------------
      // Get the data
      // Set to true to try streaming
      // Streaming is both broken and not optimized
      if(false) {
        var stream = this._db.collection(this._collectionName)
          .find(this._query, this._select)
          .stream();
        handleStream(stream);
      } else {
        this._db.collection(this._collectionName)
        .find(this._query, this._select, function (error, cursor) {
          if (error) {
            console.log(error);
            callback(error);
          }
          handleCursor(cursor);
        });
      }

      // Going to skip reprojecting
      // features.push(projectFeature(mapProjection, geoJSONDoc));
    //}.bind(this));
  }
};

module.exports = MongoSource;
