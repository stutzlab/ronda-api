#!/usr/bin/env node
"use strict";

const logger = require("./logger.js");
const utils = require("./utils.js");
const influx = require("influx");
const http = require("http");
const async = require("async");
const geolib = require("geolib");
const stats = require("stats-lite");
logger.level = "debug";

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
logger.debug("===> PROCESSING POSITIONS from accounts/" + accountId + "/positions");
influxClient.query("select * from batch_control where batch_type='position-to-presence' and accountId='"+ accountId +"' and success='true' order by time desc limit 1", function(err, results) {
  if(!err) {
    var lastSuccessTime = 0;
    if(results.length>0 && results[0].length>0) {
      lastSuccessTime = results[0][0].time;
    }
    logger.debug("Last time success = " + lastSuccessTime);
    var startTime = new Date(lastSuccessTime);

    //query positions from influxdb
    influxClient.query("select * from \"accounts/" + accountId + "/positions\" where time > "+ startTime.getTime() + " order by time asc", function(err, results) {
      if(!err) {
        if(results.length>0 && results[0].length>0) {
          processPositions(results[0], startTime, accountId, function(err, acceptedCounter, rejectedCounter) {
            var message = null;
            if(!err) {
              logger.info("Positions processed successfully. acceptedCounter=" + acceptedCounter + "; rejectedCounter=" + rejectedCounter);
            } else {
              logger.error(err + " acceptedCounter=" + acceptedCounter + "; rejectedCounter=" + rejectedCounter);
              message = err.toString();
            }
            influxClient.writePoint("batch_control", utils.deleteNullProperties({acceptedCounter:acceptedCounter, rejectedCounter:rejectedCounter, message: message}), utils.deleteNullProperties({accountId: accountId, batch_type: "position-to-presence", success: err!=null?false:true}), {}, function(err) {
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
        influxClient.writePoint("batch_control", {err:err.toString()}, {accountId: accountId, batch_type: "position-to-presence", success: false}, {}, function(err2) {
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

function processPositions(positions, startTime, accountId, callback) {
  const metricsName = "accounts/" + accountId + "/presence";
  buffer[metricsName] = [];
  var acceptedCounter = 0;
  var rejectedCounter = 0;

  //Here we're gonna reduce measured positions into presence circles
  //Presence circles are an area identified by a geo position,
  //a radius and a counter of how many times we could detect a gps position to be inside it (ronda has passed by it)

  //array of circles {latitude, longitude, totalPasses, trackerPasses} - trackerPasses[(trackerId)] = (tracker pass count)
  //presenceRegions = [{
  //  latitude: 10.00, longitude: 20.00, radius: 5,
  //  totalPasses: 44,
  //  timeBetweenPassesDeviation: 2.23,
  //  startTime: 23423423423432,
  //  endTime: 233223233223,
  //  tracker["tracker1"]: {passes: 32, time: 13000},//is this notation right?
  //  tracker["tracker2"]: {passes: 12, time: 9000}
  //}]
  const regions = [];

  //consider it to be a new pass if position detected > 3min
  const timeBetweenPasses = 180000;//180000
  //presence circle radius
  const regionRadius = 50//5

  var currentRegion = null;
  var lastPass = null;

  //CALCULATE PRESENCE REGION CIRCLES
  for(var i=0; i<positions.length; i++) {
    // try {
      var pos = {
        latitude: positions[i].latitude,
        longitude: positions[i].longitude,
        time: new Date(positions[i].time),
        trackerId: positions[i].trackerId
      }
      logger.debug("========");
      logger.debug("========");
      logger.debug("Processing position. pos=" + JSON.stringify(pos.latitude) + "," + JSON.stringify(pos.longitude) + "-" + pos.time + "; currentRegion=" + (currentRegion!=null?currentRegion.latitude:"") + "," + (currentRegion!=null?currentRegion.longitude:"") + "; lastPass=" + (lastPass!=null?lastPass.startTime:""));

      var foundRegion = null;

      //verify if point is in the same region
      //optimization tip: do this at first because the majority of queries will result on being in the same region
      if(currentRegion!=null && geolib.isPointInCircle(pos, currentRegion, regionRadius)) {
        foundRegion = currentRegion;
      }

      //find any region containing current position
      if(foundRegion==null) {
        for(var a=0; a<regions.length; a++) {
          if(geolib.isPointInCircle(pos, regions[a], regionRadius)) {
            foundRegion = regions[a];
          }
        }
      }

      //location was not found among already known regions
      if(foundRegion==null) {
        logger.debug("Creating new region");
        currentRegion = {latitude: pos.latitude, longitude: pos.longitude, radius: regionRadius, passes: [], samplesCount: 1};
        regions.push(currentRegion);
        if(lastPass!=null) {
          lastPass.endTime = pos.time;
        }
        lastPass = {startTime: pos.time, trackers: [{trackerId: pos.trackerId, time: pos.time}]};
        currentRegion.passes.push(lastPass);

      //location found among known regions
      } else if((pos.time-currentRegion.lastPosTime)<timeBetweenPasses && foundRegion!=currentRegion) {
        logger.debug("New pass on already known region");
        currentRegion = foundRegion;
        lastPass.endTime = pos.time;
        lastPass = {startTime: pos.time, trackers: [{trackerId: pos.trackerId, time: pos.time}]};

        //check to see if time between passes is too low so that a new pass on the other region is not meant to be created
        if((pos.time-currentRegion.lastPosTime)>timeBetweenPasses) {
          currentRegion.passes.push(lastPass);
        } else {
          logger.debug("Pass on the known region too near the last pass on it. Skipping.");
        }

      } else {
        logger.debug("Still on the same region + same pass");
        var sameTracker = false;
        for(var u=0; u<lastPass.trackers.length; u++) {
          if(lastPass.trackers[u].trackerId==pos.trackerId) {
            sameTracker = true;
            break;
          }
        }
        if(!sameTracker) {
          lastPass.trackers.push({trackerId: pos.trackerId, time: pos.time});
        }
      }
      currentRegion.samplesCount++;
      currentRegion.lastPosTime = pos.time;

      acceptedCounter++;

    // } catch (err) {
    //   logger.error(err);
    //   rejectedCounter++;
    // }
  }

  //CREATE DB SAMPLES
  for(var a=0; a<regions.length; a++) {
    var region = regions[a];
    var sample = [
      {time: region.passes[0].startTime, latitude: region.latitude, longitude: region.longitude, radius: region.radius, qttyPass: region.passes.length, passVariation: calculateStdev(region.passes), samplesCount: region.samplesCount, totalTime: calculateTotalTime(region.passes), passes: "\"" + JSON.stringify(region.passes) + "\""},
      {}
    ];
    utils.deleteNullProperties(sample[0]);
    utils.deleteNullProperties(sample[1]);

    logger.debug(">>>>>> ADDING REGION: " + JSON.stringify(sample));
    buffer[metricsName].push(sample);
    bufferCount++;
    //FIXME if one query returns too much data, this may overflow memory. implement async stuff here for flushing
    // if (bufferCount > 1000) {
    //   flushToInfluxDB();
    // }
  }

  influxClient.query("delete from \"accounts/" + accountId + "/presence\" where time > "+ startTime.getTime(), function(err, results) {
    if(err) {
      callback("Could not delete last presence. err=" + err, acceptedCounter, rejectedCounter);
    } else {
      logger.debug("Deleted previous presence data");
      flushToInfluxDB(function(err) {
        if(err) {
          callback("Could not flush presence. err=" + err, acceptedCounter, rejectedCounter);
        } else {
          callback(null, acceptedCounter, rejectedCounter);
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

function calculateStdev(passes) {
  const samples = [];
  var previousPass = null;
  for(var o=0; o<passes.length; o++) {
    if(o==0) {
      previousPass = passes[o];
    } else {
      samples.push(passes[o].startTime-previousPass.startTime);
      previousPass = passes[o];
    }
  }
  var result = stats.stdev(samples);
  return (isNaN(result)?null:result);
}

function calculateTotalTime(passes) {
  var total = 0;
  for(var o=0; o<passes.length; o++) {
    total += (passes[o].endTime-passes[o].startTime);
  }
  return (isNaN(total)?null:total);
}
