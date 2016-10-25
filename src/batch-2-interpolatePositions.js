#!/usr/bin/env node
"use strict";

const logger = require("./logger.js");
const utils = require("./utils.js");
const influx = require("influx");
const http = require("http");
const async = require("async");
const geolib = require("geolib");
const sgeo = require("sgeo");
const stats = require("stats-lite");
const d3 = require("d3-interpolate");
logger.level = "debug";

const minDistance = 25;
const maxDistance = 500;
const maxTime = 60000;

//prepare influxdb communications
logger.info("Starting Ronda position2presence Batch");
logger.info("");

const influxClient = utils.getInfluxClient();
var buffer = {};
var bufferCount = 0;

//list of accounts to be processed
//TODO get from command line parameter
const accountId = "flaviostutz";

//TRANSFORM POSITIONS TO PRESENCE INFO
logger.debug("");
logger.debug("===> INTERPOLATING POSITIONS from accounts/" + accountId + "/positions");
influxClient.query("select * from batch_control where batch_type='position-interpolation' and accountId='"+ accountId +"' and success='true' order by time desc limit 1", function(err, results) {
  if(!err) {
    var lastSuccessTime = 0;
    if(results.length>0 && results[0].length>0) {
      lastSuccessTime = results[0][0].lastElementTime;
    }
    logger.debug("Last time success = " + lastSuccessTime);
    var startTime = new Date(lastSuccessTime);

    //query positions from influxdb
    logger.info("Getting position data to be the source of interpolation");
    influxClient.query("select * from \"accounts/" + accountId + "/positions\" where time >= "+ startTime.getTime() + "s and interpolated<>'true' order by time asc", function(err, results) {
      if(!err) {
        if(results.length>0 && results[0].length>0) {
          interpolatePositions(results[0], startTime, accountId, function(err, acceptedCounter, rejectedCounter, lastElementTime) {
            var message = null;
            if(!err) {
              logger.info("Interpolation processed successfully. acceptedCounter=" + acceptedCounter + "; rejectedCounter=" + rejectedCounter + "; lastElementTime=" + lastElementTime);
            } else {
              logger.error(err + " acceptedCounter=" + acceptedCounter + "; rejectedCounter=" + rejectedCounter);
              message = err.toString();
            }
            influxClient.writePoint("batch_control", utils.deleteNullProperties({acceptedCounter:acceptedCounter, rejectedCounter:rejectedCounter, message: message, lastElementTime: lastElementTime}), utils.deleteNullProperties({accountId: accountId, batch_type: "position-to-presence", success: err!=null?false:true}), {}, function(err) {
              if(err) {
                logger.error(err);
                logger.error("Error writing batch_control success state. err=" + err);
              } else {
                logger.debug("Successfully wrote batch_control success state");
              }
            });
          });
        } else {
          logger.debug("Empty results for positions found. Skipping.");
        }
      } else {
        logger.error(err);
        influxClient.writePoint("batch_control", {err:err.toString()}, {accountId: accountId, batch_type: "position-interpolation", success: false}, {}, function(err2) {
          if(err2) {
            logger.error("Error writing batch_control failure state. err=" + err2);
          } else {
            logger.debug("Wrote exception error to batch_control successfully");
          }
        });
      }
    });

  } else {
    logger.error("Could not process positions for account " + accountId + ". cause=" + err);
  }
});

function interpolatePositions(rawPositions, startTime, accountId, callback) {
  const metricsName = "accounts/" + accountId + "/positions";
  buffer[metricsName] = [];
  var acceptedCounter = 0;
  var rejectedCounter = 0;
  var lastElementTime = null;

  //INTERPOLATE POSITIONS
  logger.debug("POSITION INTERPOLATION FOR ACCOUNT " + accountId);

  //separate positions by trackerId
  var positionsByTracker = {};
  for(var i=0; i<rawPositions.length; i++) {
    var pos = rawPositions[i];
    if(positionsByTracker[pos.trackerId]==null) {
      positionsByTracker[pos.trackerId] = [];
    }
    positionsByTracker[pos.trackerId].push(pos);
    lastElementTime = pos.time;
  }

  for(var trackerId in positionsByTracker) {
    logger.debug("Processing "+ positionsByTracker[trackerId].length +" positions for " + trackerId);
    var prevPosition = null;
    for(var i=0; i<positionsByTracker[trackerId].length; i++) {
      var position = positionsByTracker[trackerId][i];
      var distance = -1;
      var time = -1;

      if(prevPosition!=null) {
        distance = geolib.getDistance(position, prevPosition);
        time = new Date(position.time).getTime()-new Date(prevPosition.time).getTime();
      }

      //contiguous measurements between distant points. interpolate
      if(prevPosition!=null && time<maxTime && distance>minDistance && distance<maxDistance) {
        var qtty = Math.round(distance/minDistance);
        //d3 interpolator returns NaN lat/lng when there is just one point
        // if(qtty<=1) {
        //   qtty=2;
        // }
        logger.debug("Adding "+ qtty +" interpolated positions between " + prevPosition.time + " and " + position.time);

        var timeI = d3.interpolateNumber(new Date(prevPosition.time).getTime(), new Date(position.time).getTime());
        var altitudeI = d3.interpolateNumber(prevPosition.altitude, position.altitude);
        var speedI = d3.interpolateNumber(prevPosition.speed, position.speed);
        var trackI = d3.interpolateNumber(prevPosition.track, position.track);
        var prevPoint = new sgeo.latlon(prevPosition.latitude, prevPosition.longitude);
        var point = new sgeo.latlon(position.latitude, position.longitude);
        var interpolatedPositions = prevPoint.interpolate(point, qtty+2);
        var interpolationRatio = 1/(qtty+1);

        for(var r=1; r<(interpolatedPositions.length-1); r++) {
          var fraction = interpolationRatio*r;
          //time	altitude	hdop	latitude	longitude	qttySatellites	speed	track	trackerId
          var sample = [
            {
              time: timeI(fraction),
              altitude: altitudeI(fraction),
              hdop: prevPosition.hdop,
              latitude: interpolatedPositions[r].lat,
              longitude: interpolatedPositions[r].lng,
              qttySatellites: prevPosition.qttySatellites,
              speed: speedI(fraction),
              track: trackI(fraction)
            },
            {
              trackerId: prevPosition.trackerId,
              interpolated: 'true'
            }
          ];

          logger.debug("Adding " + JSON.stringify(sample));
          utils.deleteNullProperties(sample[0]);
          utils.deleteNullProperties(sample[1]);
          buffer[metricsName].push(sample);
          bufferCount++;

          acceptedCounter++;
        }

      //non contiguous measurements
      } else {
        // logger.debug("Non interpolatable measurements. Skipping.");
        rejectedCounter++;
      }

      prevPosition = position;
    }
  }

  influxClient.query("delete from \"accounts/" + accountId + "/positions\" where time >= "+ startTime.getTime() + "s and interpolated='true'", function(err, results) {
    if(err) {
      callback("Could not delete previously interpolated positions. err=" + err, acceptedCounter, rejectedCounter);
    } else {
      logger.debug("Deleted previous interpolated positions");
      flushToInfluxDB(function(err) {
        if(err) {
          callback("Could not flush interpolated positions. err=" + err, acceptedCounter, rejectedCounter, null);
        } else {
          callback(null, acceptedCounter, rejectedCounter, lastElementTime);
        }
      });
    }
  });

}

function flushToInfluxDB(callback) {
  if (bufferCount==0) {
    callback();
  } else {
    influxClient.writeSeries(buffer, {}, function (err, res) {
        if (err) {
          logger.error(err);
        } else {
          logger.info("Flushed " + bufferCount + " points to InfluxDB");
        }
        buffer = {};
        bufferCount = 0;
        callback(err);
    });
  }
}
