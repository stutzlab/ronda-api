module.exports = {
  getInfluxClient : function() {
    const influxdb_url = process.env.INFLUXDB_HOST || "http://influxdb.presence.ronda.stutzthings.com:8086";
    logger.info("influxdb_url: " + influxdb_url);
    return influx({
      hosts: [influxdb_url],
      username: process.env.INFLUXDB_USERNAME || "admin",
      password: process.env.INFLUXDB_PASSWORD || "admin",
      database: process.env.INFLUXDB_DB || "ronda_presence"
    })
  }
};
