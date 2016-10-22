"use strict";
const logger = require("./logger.js");
const influx = require("influx");

module.exports = {
  getInfluxClient : function() {
//    const influxdb_url = process.env.INFLUXDB_HOST || "http://influxdbapi.presence.ronda.stutzthings.com:8086";
    const influxdb_url = process.env.INFLUXDB_HOST || "http://influxdbapi.data.stutzthings.com:8086";
    logger.info("influxdb_url: " + influxdb_url);
    return influx({
      hosts: [influxdb_url],
      username: process.env.INFLUXDB_USERNAME || "admin",
      password: process.env.INFLUXDB_PASSWORD || "admin",
      database: process.env.INFLUXDB_DB || "ronda_presence"
    })
  },
  deleteNullProperties : function(object) {
    for(var propertyName in object) {
      if(object[propertyName] == null) {
        delete object[propertyName];
      }
    }
    return object;
  }
};
