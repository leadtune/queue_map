require "spec_helper"

def timing(&block)
  time = Time.now
  yield
  (Time.now - time)
end

describe "Integration" do
  context "Running consumers in separate processes" do
    def start_consumers
      @process = BackgroundProcess.run("#{BIN_PATH}/queue_map_consumer -f start #{SPEC_PATH}/support/greet_consumer.rb")
      @process.detect { |line| /Starting consumers/.match(line) }
      sleep 0.1 # give processes time to connect to queue server and be fully setup..
    end

    def stop_consumers(graceful = true)
      @process && @process.kill(graceful ? "INT" : "TERM")
    end

    after(:each) do
      stop_consumers
    end

    it "performs the work in the spawned processes" do
      start_consumers
      ['Bob', 'Jim', 'Charlie'].queue_map(:greet).should == ['Hello, Bob', 'Hello, Jim', 'Hello, Charlie']
    end

    it "runs the :on_timeout proc if result not received within :timeout seconds" do
      input = [SlowResponse.new("Bob", 1.2), SlowResponse.new("Jim", 0.2)]
      input.queue_map(:greet, :timeout => 1, :on_timeout => lambda { |r| "No time for you, #{r.name}" }).should == ["No time for you, Bob", "Hello, Jim"]
    end

    it "outputs exceptions, as raised, and continues consuming messages" do
      log_file = QueueMap.consumer(:greet).log_file
      File.truncate(log_file)
      gravel = (1..3).map { |i| Surprise.new("Surprise #{i}!") }
      gravel.queue_map(:greet, :timeout => 1.5)
      log_contents = File.read(log_file)
      (1..3).each do |i|
        log_contents.should include("Surprise #{i}!")
      end
      ['Bob', 'Jim'].queue_map(:greet, :timeout => 5).should == ['Hello, Bob', 'Hello, Jim']
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
end
