#!/usr/bin/env node

const logger = require("./logger.js");
const utils = require("./utils.js");
const influx = require("influx");
logger.level = "debug";

//prepare influxdb communications
logger.info("Starting Ronda Presence Advisor API");
logger.info("");

//start http server
var Hapi = require("hapi");
var server = new Hapi.Server();

const influxClient = utils.getInfluxClient();

server.connection({port: 80});

//HISTORICAL DATA ACCESS
server.route({
  method: "GET",
  path: "/v1/{account_id}/{tracker_id}",
  handler: function(req, reply) {
    // logger.debug("Token: client_id="+ client_id + "; resource_owner=" + resource_owner + "; scopes=" + scopes);

    const raw_metrics_name = "v1/" + req.params.account_id + "/" + req.params.tracker_id + "/gps/raw";
    logger.debug("metrics_name="+ metrics_name);

    //const authorized = utils.isAppAuthorizedDeviceRead(req.payload.scopes, req.params.account_id);
    //logger.debug("isAppAuthorizedDeviceRead="+ authorized);

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

    } else {
      reply({message:"Unauthorized access to account data. account_id=" + req.params.account_id})
        .code(401)
        .header("Content-Type", "application/json");
    }
  }
});

server.route({
  method: "GET",
  path: "/health",
  handler: function(req, reply) {
    reply("OK");
  }
});

server.route({
  method: "*",
  path: "/{p*}",
  handler: function (req, reply) {
    return reply({message:"Resource not found"}).code(404);
  }
});

server.start(function(){ // boots your server
  console.log("stutzthings-data-api started on port 4000");
});

module.exports = server;
