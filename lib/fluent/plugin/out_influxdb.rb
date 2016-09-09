# encoding: UTF-8
require 'date'
require 'influxdb'
require 'fluent/mixin'

class Fluent::InfluxdbOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('influxdb', self)

  include Fluent::HandleTagNameMixin

  config_param :host, :string,  :default => 'localhost',
               :desc => "The IP or domain of influxDB."
  config_param :port, :integer,  :default => 8086,
               :desc => "The HTTP port of influxDB."
  config_param :dbname, :string,  :default => 'fluentd',
               :desc => <<-DESC
The database name of influxDB.
You should create the database and grant permissions at first.
DESC
  config_param :user, :string,  :default => 'root',
               :desc => "The DB user of influxDB, should be created manually."
  config_param :password, :string,  :default => 'root', :secret => true,
               :desc => "The password of the user."
  config_param :retry, :integer, :default => nil,
               :desc => 'The finite number of retry times. default is infinite'
  config_param :time_key, :string, :default => 'time',
               :desc => 'Use value of this tag if it exists in event instead of event timestamp'
  config_param :time_precision, :string, :default => 's',
               :desc => <<-DESC
The time precision of timestamp.
You should specify either hour (h), minutes (m), second (s),
millisecond (ms), microsecond (u), or nanosecond (n).
DESC
  config_param :use_ssl, :bool, :default => false,
               :desc => "Use SSL when connecting to influxDB."
  config_param :verify_ssl, :bool, :default => true,
               :desc => "Enable/Disable SSL Certs verification when connecting to influxDB via SSL."

  def initialize
    super
    @seq = 0
    @prev_timestamp = nil
  end

  def configure(conf)
    super
  end

  def start
    super

    $log.info "Connecting to database: #{@dbname}, host: #{@host}, port: #{@port}, username: #{@user}, use_ssl = #{@use_ssl}, verify_ssl = #{@verify_ssl}"

    # ||= for testing.
    @influxdb ||= InfluxDB::Client.new @dbname, host: @host,
                                                port: @port,
                                                username: @user,
                                                password: @password,
                                                async: false,
                                                retry: @retry,
                                                time_precision: @time_precision,
                                                use_ssl: @use_ssl,
                                                verify_ssl: @verify_ssl

    begin
      existing_databases = @influxdb.list_databases.map { |x| x['name'] }
      unless existing_databases.include? @dbname
        raise Fluent::ConfigError, 'Database ' + @dbname + ' doesn\'t exist. Create it first, please. Existing databases: ' + existing_databases.join(',')
      end
    rescue InfluxDB::AuthenticationError
      $log.info "skip database presence check because '#{@user}' user doesn't have admin privilege. Check '#{@dbname}' exists on influxdb"
    end
  end

  FORMATTED_RESULT_FOR_INVALID_RECORD = ''.freeze

  def format(tag, time, record)
    # TODO: Use tag based chunk separation for more reliability
    if record.empty? || record.has_value?(nil)
      FORMATTED_RESULT_FOR_INVALID_RECORD
    else
      [tag, time, record].to_msgpack
    end
  end

  def shutdown
    super
    @influxdb.stop!
  end

  def write(chunk)
    points = []
    chunk.msgpack_each do |tag, time, record|
      timestamp = record.delete(@time_key) || time
      
      # prep the placement tags
      tags = {}
      if record.has_key? 'service'
        tags['service'] = record['service']
      end

      %w(cloud hostname instanceid podname region zone).each do |placement_tag|
        if record.has_key? "placement.#{placement_tag}"
          tags[placement_tag] = record['placement.' + placement_tag]
        end
      end

      points = []
      record.each_key do |key_name|
        if record.has_key? 'schema'
          case record['schema']
          when 'woodpecker.v1'
            if key_name.match /<(int|long)>$/
              points << {
                :timestamp => timestamp.to_i,
                :series    => record['module'] << "." << record['submodule'] << "." << record['action'] << "." << key_name.sub(/<(int|long)>$/, ''),
                :values    => { value: record[key_name].to_f },
                :tags      => tags,
              }
            elsif key_name.match /<(float|double)>$/
              points << {
                :timestamp => timestamp.to_i,
                :series    => record['module'] << "." << record['submodule'] << "." << record['action'] << "." << key_name.sub(/<(float|double)>$/, ''),
                :values    => { value: record[key_name].to_f },
                :tags      => tags,
              }
            end
          else
            $log.warn('Unrecognized schema. Dropping record.')
          end
        else
          if key_name.match /<(int|long)>$/
            points << {
              :timestamp => timestamp.to_i,
              :series    => key_name.sub(/<(int|long)>$/, ''),
              :values    => { value: record[key_name].to_f },
              :tags      => tags,
            }
          elsif key_name.match /<(float|double)>$/
            points << {
              :timestamp => timestamp.to_i,
              :series    => key_name.sub(/<(float|double)>$/, ''),
              :values    => { value: record[key_name].to_f },
              :tags      => tags,
            }
          end
        end
      end

      unless points.empty?
        @influxdb.write_points(points, nil)
      end
    end
  end
end
