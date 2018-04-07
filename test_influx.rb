require 'influxdb'
influxdb = InfluxDB::Client.new
# username       = 'foo'
# password       = 'bar'
database       = 'mydb'
name           = 'foobar'
time_precision = 's'

# either in the client initialization:
influxdb = InfluxDB::Client.new database, time_precision: time_precision
  # username: username,
  # password: password,
  

data = {
  values: { value: 0 },
  timestamp: Time.now.to_i # timestamp is optional, if not provided point will be saved with current time
}

influxdb.write_point(name, data)

# or in a method call:
influxdb.write_point(name, data, time_precision)