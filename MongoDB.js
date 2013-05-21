var MongoClient = require('mongodb').MongoClient;
var projector = require(__dirname + "/../lib/projector");
var Response = require('./ResponseModel');
var Form = require('./FormModel');
var __ = require("lodash");


var MongoSource = function(options) {
  this._projection = projector.util.cleanProjString(options.projection || 'EPSG:4326');
  this._connectionString = options.connectionString; // required
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
      // console.log(min,max);
    }

    var query = this._query || {};
    var parsedBbox = [[min[0], min[1]], [max[0],  max[1]]];
    query[this._geoKey] = { '$within': { '$box': parsedBbox } };

    MongoClient.connect(this._connectionString, function(err, db) {
      if(err) {
        console.log("Mongo error:", err);
      }

      start = Date.now();
      var stream = db.collection(this._collectionName)
        .find(query, this._select)
        .stream();

      var cursor = db.collection(this._collectionName)
        .find(query, selectConditions);

      var features = [];

      var self = this;

      /**
       * Turn stored a stored parcel result into geoJSON
       * TODO: Can save time and memory by not creating a new object here.
       * @param  {Object} item  A single response
       * @return {Object}       A single response structured as geoJSON
       */
      function resultToGeoJSON(item, filter) {
        var i;
        var obj;
        var newItems = [];

        obj = {};
        obj.type = 'Feature';

        // Get the shape
        // Or if there isn't one, use the centroid.
        if (item.geo_info.geometry !== undefined) {
          obj.id = item.parcel_id;
          obj.geometry = item.geo_info.geometry;
        }else {
          obj.id = item._id;
          obj.geometry = {
            type: 'Point',
            coordinates: item.geo_info.centroid
          };
        }

        obj.properties = item;

        // If there is a filer, we also want the key easily accessible.
        if(filter) {
          // TODO: Handle the undefined condition
          if(item.hasOwnProperty("responses")) {
            if(item.responses.hasOwnProperty(filter.key)) {
              obj.properties[filter.key] = item.responses[filter.key];
            }
            // else {
            //   obj.properties[filter.key] = 'undefined';
            // }
          }
          // else {
          //   obj.properties[filter.key] = 'undefined';
          // }
        }

        // Project the object
        if (self._projection !== mapProjection){
          return projector.project.Feature(self._projection, mapProjection, obj);
        }
        return obj;
      }

      function addFeature(doc) {
        features.push(resultToGeoJSON(doc));
      }

      // ----------------------------------------------------------
      // Get the data
      // Set to true to try streaming
      if(false) {
        stream.on('data', addFeature);

        stream.on('close', function() {
          console.log("Fetched and processed " + features.length + " responses in " + (Date.now() - start) + "ms");

          callback(null, {
            type: 'FeatureCollection',
            features: features
          });
        }.bind(this));
      }else {

        // The same thing as above, except using toArray
        // NB -- conversion to a format that the renderer wants is broken
        cursor.toArray(function(error, results) {
          console.log("Fetched " + results.length + " responses in " + (Date.now() - start) + "ms");

          start = Date.now();
          var features = [];
          var len = results.length;
          for (var i = 0; i < len; i++) {
            features.push(resultToGeoJSON(results[i]));
          }
          console.log("Processed " + results.length + " responses in " + (Date.now() - start) + "ms");
          callback(null, {
            type: 'FeatureCollection',
            features: features
          });

        }.bind(this));

      }

      // Going to skip reprojecting
      // features.push(projectFeature(mapProjection, geoJSONDoc));
    }.bind(this));
  }
};

module.exports = MongoSource;
