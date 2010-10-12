require File.dirname(__FILE__) + "/ext/enumerable.rb"
require "bunny"
require 'timeout'

module QueueMap
  autoload :Consumer, File.dirname(__FILE__) + "/queue_map/consumer"
  BUNNY_MUTEX = Mutex.new
  extend self
  attr_accessor :mode, :consumer_path
  attr_writer :consumer_base_path
  attr_accessor
  attr_reader :connection_info

  DEFAULT_ON_TIMEOUT = lambda { |r| nil }

  def unique_name
    @unique_name ||= "#{`hostname`.chomp}-#{Process.pid}-#{Time.now.usec}"
  end

  def consumer_base_path
    @consumer_base_path ||= "lib/consumers"
  end

  def consumer_path
    @consumer_path ||= Hash.new do |hash, key|
      File.join(consumer_base_path, "#{key}_consumer.rb")
    end
  end

  def response_queue_name(name)
    @inc ||= 0
    "#{name}_response_#{unique_name}_#{@inc += 1}"
  end

  def map(collection, name, options = {})
    return queue_map_internal(collection, name) if mode == :test

    with_bunny do |bunny|
      q = bunny.queue(name.to_s)
      response_queue_name = response_queue_name(name)
      response_queue = bunny.queue(response_queue_name, :durable => false, :exclusive => true, :auto_delete => true)

      (0..(collection.length - 1)).each do |i|
        q.publish(Marshal.dump(:input => collection[i], :index => i, :response_queue => response_queue_name))
      end

      results = {}
      begin
        Timeout.timeout(options[:timeout] || 5) do
          collection.length.times do
            sleep 0.05 while (next_response = response_queue.pop) == :queue_empty
            response = Marshal.load(next_response)
            results[response[:index]] = response[:result]
          end
        end
      rescue Timeout::Error => e
      end

      (0..(collection.length - 1)).map do |i|
        results[i] || (options[:on_timeout] || DEFAULT_ON_TIMEOUT).call(collection[i])
      end
    end
  end

  def queue_map_internal(collection, name, *args)
    collection.map(&consumer(name).worker_proc)
  end

  def consumers
    @consumers ||= { }
  end

  def consumer(name)
    consumers[name] ||= QueueMap::Consumer.from_file(consumer_path[name], :strategy => mode || :thread)
  end

  def with_bunny(&block)
    bunny = nil
    BUNNY_MUTEX.synchronize do
      bunny = Bunny.new((@connection_info || { }).merge(:spec => '08'))
      bunny.start
    end
    begin
      yield bunny
    ensure
      (bunny.close_connection unless bunny.status == :not_connected) rescue nil
    end
  end

  def connection_info=(connection_info)
    @bunny = nil
    @connection_info = connection_info
  end

  def mode=(mode)
    @mode = mode
    consumers.clear
  end
end
