#!/usr/bin/env node

const logger = require("./logger.js");
const utils = require("./utils.js");
const influx = require("influx");
logger.level = "debug";

//prepare influxdb communications
logger.info("Starting Ronda Presence Advisor Batch");
logger.info("");

const influxClient = utils.getInfluxClient();

//list of trackers/account to be processed
//TODO get from command line parameter
const account_id = "flaviostutz";
const tracker_id = "23423423423";

//reduce raw gps data into presence heat map
influxClient.query("select time from batch_raw_gps where tracker_id = \'"+ tracker_id +"\' and success = true limit 1", function(err, results) {
  if(!err) {
    var start_time = null;
    if(results.length>0) {
      start_time = results[0].time;
    }
    logger.debug("Last time batch was processed successfully for tracker " + tracker_id + " was " + start_time);

    //query historical raw gps data from StutzThings
parei aqui!


  } else {
    logger.error("Could not process raw gps data for tracker " + tracker_id + ". cause=" + err);
  }
});

    //if(authorized) {
    if(true) {
      //query data from influxdb
      const start_time = req.query.start_time;
      const end_time = req.query.end_time;
      var influx_query = "";
      if(start_time || end_time) {
        influx_query = " where ";
        influx_query += (start_time?" time >= \'" + start_time + "\'":"");
        if(start_time && end_time) {
          influx_query += " and ";
        }
        influx_query += (end_time?" time <= \'" + end_time + "\'":"");
      }

      influxClient.query("select * from \""+ metrics_name +"\" " + influx_query, function(err, results) {
        if(!err) {
          reply(results)
            .code(200)
            .header("Content-Type", "application/json");
        } else {
          reply({message:"Error quering data. cause=" + err})
            .code(500)
            .header("Content-Type", "application/json");
        }
      });

module.exports = server;
