#!/usr/bin/env node
"use strict";

const logger = require("./logger.js");
const utils = require("./utils.js");
const influx = require("influx");
const http = require("http");
const async = require("async");
const nmea = require("node-nmea");
logger.level = "debug";

//prepare influxdb communications
logger.info("Starting Ronda rawgps2position Batch");
logger.info("");

const influxClient = utils.getInfluxClient();
var buffer = {};
var bufferCount = 0;

//list of trackers/account to be processed
//TODO get from command line parameter
const account_id = "flaviostutz";
const tracker_id = "homie1";

//TRANSFORM RAW GPS DATA TO POSITIONS
logger.debug("");
logger.debug("===> PROCESSING RAW GPS DATA FOR /v1/devices/" + account_id + "/" + tracker_id + "/gps/raw");
influxClient.query("select * from batch_control where batch_type='raw-gps-to-position' and account_id='"+ account_id +"' and tracker_id='"+ tracker_id +"' and success='true' order by time desc limit 1", function(err, results) {
  if(!err) {
    var startTime = null;
    if(results.length>0 && results[0].length>0) {
      startTime = new Date(results[0][0].time);
    }
    logger.debug("Last time success = " + startTime);

    //query historical raw gps data from StutzThings
    var deviceNode = "v1/devices/" + account_id + "/" + tracker_id + "/gps";
    var metricsName = deviceNode + "/position";
    logger.debug("Getting data from " + metricsName + "; startTime=" + startTime);
    http.get({
      host: "api.data.stutzthings.com",
      path: "/"+ deviceNode +"/raw?start_time=" + (startTime!=null?startTime:"")
    }, function(response) {
      // Continuously update stream with data
      var body = '';
      response.on('data', function(d) {
        body += d;
      });
      response.on('end', function() {
        try {
          processRawGPS(body, account_id, tracker_id, startTime, function(err, acceptedCounter, rejectedCounter) {
            var message = null;
            if(!err) {
              logger.info("Raw gps processed successfully. acceptedCounter=" + acceptedCounter + "; rejectedCounter=" + rejectedCounter);
            } else {
              logger.error(err + " acceptedCounter=" + acceptedCounter + "; rejectedCounter=" + rejectedCounter);
              message = err.toString();
            }
            influxClient.writePoint("batch_control", utils.deleteNullProperties({acceptedCounter:acceptedCounter, rejectedCounter:rejectedCounter, message: message}), utils.deleteNullProperties({account_id: account_id, tracker_id: tracker_id, batch_type: "raw-gps-to-position", success: err!=null?false:true}), {}, function(err) {
              if(err) {
                logger.error(err);
                logger.error("Error writing batch_control. err=" + err);
              } else {
                logger.debug("Successfully wrote batch_control");
              }
            });
          });
        } catch (err) {
          logger.error(err);
          influxClient.writePoint("batch_control", {err:err.toString()}, {account_id: account_id, tracker_id: tracker_id, batch_type: "raw-gps-to-position", success: false}, {}, function(err2) {
            if(err2) {
              logger.error("Error writing batch_control failure state. err=" + err2);
            } else {
              logger.debug("Wrote exception error to batch_control successfully");
            }
          });
        }
      });
      response.on('error', function(e) {
        logger.error("Error reading StutzThings data api. err=" + e.message);
        influxClient.writePoint("batch_control", {message: e.message}, {account_id: account_id, tracker_id: tracker_id, batch_type: "raw-gps-to-position", success: false}, {}, function(err) {
          if(err) {
            logger.error("Error writing batch_control failure state. err=" + err);
          }
        });
      });
    });

  } else {
    logger.error("Could not process raw gps data for tracker " + tracker_id + ". cause=" + err);
  }
});

function processRawGPS(rawGPSData, accountId, trackerId, startTime, callback) {
  var metricsName = "accounts/"+ accountId + "/positions";
  var parsed = JSON.parse(rawGPSData);
  buffer[metricsName] = [];
  var acceptedCounter = 0;
  var rejectedCounter = 0;
  var lastRmcNmea = null;
  var lastGgaNmea = null;
  var saveSample = null;

  logger.debug("Processing raw gps data. accountId=" + accountId + "; trackerId=" + trackerId + "; samples=" + parsed[0].length);

  // logger.debug("rawGPSData = " + JSON.stringify(parsed));
  for(var i=0; i<parsed[0].length; i++) {
    var rawGps = parsed[0][i].valueStr;
    logger.debug("========");
    logger.debug("========");
    // logger.info("Processing raw gps data: " + rawGps);
    logger.info("Processing raw gps data");
    var lines = rawGps.split("\r");
    lines = lines.concat(rawGps.split("\n"));
    for(var a=0; a<lines.length; a++) {
      logger.debug("---------");
      var line = lines[a].trim();
      if(line.length>0) {
        // logger.debug("RAW NMEA: " + line);
        try {
          const nmeaMsg = nmea.parse(line);
          // logger.debug("PARSED NMEA: " + JSON.stringify(nmeaMsg));

          if(nmeaMsg.valid && nmeaMsg.datetime!=null && !isNaN(nmeaMsg.datetime.getTime())) {
            var sample = null;

            if(nmeaMsg.type == "RMC") {
              if(nmeaMsg.track!=null) {
                nmeaMsg.track = parseFloat(nmeaMsg.track);
              }
              if(nmeaMsg.track=="") {
                nmeaMsg.track = null;
              }
              sample = [
                {time: nmeaMsg.datetime.getTime(), latitude: nmeaMsg.loc.geojson.coordinates[1], longitude: nmeaMsg.loc.geojson.coordinates[0], speed: nmeaMsg.speed.kmh, track: nmeaMsg.track},
                {gpsQuality: nmeaMsg.gpsQuality, trackerId: trackerId}
              ];
              utils.deleteNullProperties(sample[0]);
              utils.deleteNullProperties(sample[1]);
              lastRmcNmea = nmeaMsg;

            } else if (nmeaMsg.type == "GGA") {
              if(lastRmcNmea!=null) {
                //gga samples doesn't come with day-month-year info. try to use previous rmc message date
                nmeaMsg.datetime.setDate(lastRmcNmea.datetime.getDate());
                nmeaMsg.datetime.setMonth(lastRmcNmea.datetime.getMonth());
                nmeaMsg.datetime.setFullYear(lastRmcNmea.datetime.getFullYear());
                //if datetime is too far from last rmc message, something may be wrong (samples are probably too distant and we should not use last rmc day month year part)
                if(Math.abs(nmeaMsg.datetime.getTime()-lastRmcNmea.datetime.getTime())<600000) {
                  sample = [
                    {time: nmeaMsg.datetime.getTime(), latitude: nmeaMsg.loc.geojson.coordinates[1], longitude: nmeaMsg.loc.geojson.coordinates[0], altitude: nmeaMsg.altitude, qttySatellites: nmeaMsg.satellites, hdop: nmeaMsg.hdop},
                    {gpsQuality: nmeaMsg.gpsQuality, trackerId: trackerId}
                  ];
                  utils.deleteNullProperties(sample[0]);
                  utils.deleteNullProperties(sample[1]);
                }
              }
              lastGgaNmea = nmeaMsg;
            }

            if(sample!=null) {
              logger.debug(">>>>>> ACCEPTED SAMPLE: " + JSON.stringify(sample));

              if(saveSample!=null) {

                //if sample has the same timestamp as the previous sample, merge the two
                if (saveSample[0].time==sample[0].time) {
                  logger.debug("Merging two samples with the same timestamp. " + JSON.stringify(saveSample) + " ----- " + JSON.stringify(sample));
                  for (var attrname in sample[0]) {
                    saveSample[0][attrname] = sample[0][attrname];
                  }
                  for (var attrname in sample[1]) {
                    saveSample[1][attrname] = sample[1][attrname];
                  }

                } else {
                  logger.debug("Saving sample " + JSON.stringify(saveSample));
                  buffer[metricsName].push(saveSample);
                  bufferCount++;
                  acceptedCounter++;

                  saveSample = sample;
                }

              } else {
                saveSample = sample;
              }

            } else {
              logger.debug("Rejected sample (GGA sample too far from last RMC): " + JSON.stringify(nmeaMsg));
              rejectedCounter++;
            }

          } else {
            logger.debug("Rejected sample (invalid nmea or datetime): " + JSON.stringify(nmeaMsg));
            rejectedCounter++;
          }
        } catch (err) {
          logger.error("Rejected sample (error): " + line + ". error=" + err);
          rejectedCounter++;
        }
      }
    }
    logger.debug("Pending samples: " + bufferCount);
  }

  //save last pending sample
  if(saveSample!=null) {
    buffer[metricsName].push(saveSample);
    bufferCount++;
    acceptedCounter++;
  }

  //delete previous positions
  logger.debug("Deleting previous position data from accounts/" + accountId + "/positions fromDate=" + startTime);
  influxClient.query("delete from \"accounts/" + accountId + "/positions\" where time >= "+ (startTime!=null?startTime.getTime():0) + "s", function(err, results) {
    if(err) {
      callback("Could not delete existing positions. err=" + err, acceptedCounter, rejectedCounter);
    } else {
      logger.debug("Deleted existing positions after " + startTime);
      flushToInfluxDB(function(err) {
        if(err) {
          callback("Could not process raw gps positions. err=" + err, acceptedCounter, rejectedCounter);
        } else {
          callback(null, acceptedCounter, rejectedCounter);
        }
      });
    }
  });

}

function flushToInfluxDB(callback) {
  logger.debug("Flushing data to influxdb. bufferCount=" + bufferCount);
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
