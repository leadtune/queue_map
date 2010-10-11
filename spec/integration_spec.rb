require "spec_helper"

def timing(&block)
  time = Time.now
  yield
  (Time.now - time)
end

class Surprise
  def to_s
    raise "Surprise!"
  end
end

class SlowResponse
  attr_reader :name
  def initialize(name, sec)
    @name, @sec = name, sec
  end

  def to_s
    sleep @sec
    @name
  end
end

describe "Integration" do
  context "Running consumers in separate processes" do
    def start_consumers
      @process = BackgroundProcess.run("#{BIN_PATH}/queue_map_consumer -f #{SPEC_PATH}/support/greet_consumer.rb")
      @process.detect { |line| /Starting consumers/.match(line) }
    end

    def stop_consumers(graceful = true)
      @process && @process.kill(graceful ? "INT" : "TERM")
    end

    after(:each) do
      stop_consumers
    end

    it "does stuff" do
      start_consumers
      ['Bob', 'Jim', 'Charlie'].queue_map(:greet).should == ['Hello, Bob', 'Hello, Jim', 'Hello, Charlie']
    end
  end

  context "Running the consumer in test mode" do
    before(:each) do
      QueueMap.mode = :test
    end

    after(:each) do
      QueueMap.mode = nil
    end

    it "runs the consumer directly, without using the queue" do
      results = ['Bob', 'Jim'].queue_map(:greet)
      results.should == ['Hello, Bob', 'Hello, Jim']
    end
  end

  context "Running the consumers in threads" do
    before(:each) do
      QueueMap.mode = :thread
      @consumer = QueueMap.consumer(:greet)
      @consumer.start
    end

    after(:each) do
      @consumer.stop
      QueueMap.mode = nil
    end

    it "runs the consumer using the queue" do
      results = ['Bob', 'Jim'].queue_map(:greet)
      results.should == ['Hello, Bob', 'Hello, Jim']
    end

    it "runs the :on_timeout proc if result not received within :timeout seconds" do
      input = [SlowResponse.new("Bob", 1.2), SlowResponse.new("Jim", 0.2)]
      input.queue_map(:greet, :timeout => 1, :on_timeout => lambda { |r| "No time for you, #{r.name}" }).should == ["No time for you, Bob", "Hello, Jim"]
    end

    it "continues consuming messages if exceptions are raised" do
      exceptions = []
      @consumer.on_exception_proc = lambda { |e| exceptions << e }
      gravel = [Surprise.new] * 3
      gravel.queue_map(:greet, :timeout => 1.5)
      exceptions.map { |e| e.message }.should == %w[Surprise! Surprise! Surprise!]
      ['Bob', 'Jim'].queue_map(:greet, :timeout => 5).should == ['Hello, Bob', 'Hello, Jim']
    end
  end
end
