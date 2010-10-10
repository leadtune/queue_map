require "spec_helper"

def timing(&block)
  time = Time.now
  yield
  (Time.now - time)
end

describe "Integration" do
  context "Running consumers in separate processes" do
    def start_consumers
      @process = BackgroundProcess.run("#{BIN_PATH}/queue_map_consumer -f #{SPEC_PATH}/support/greet_consumer.rb")
      @process.detect { |line| /ready/.match(line) }
    end

    def stop_consumers
      @process && @process.kill
    end

    after(:each) do
      stop_consumers
    end

    it "does stuff" do
      pending
      start_consumers
      timing {
        results = ['Bob', 'Jim', 'dork'].queue_map(:greet)
        results.should == ['Hello, Bob', 'Hello, Jim', 'Hello, dork']
      }.should be_close(2, 0.3)
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
      time = Time.now
      results = ['Bob', 'Jim'].queue_map(:greet)
      results.should == ['Hello, Bob', 'Hello, Jim']
      (Time.now - time).should < 3
    end
  end

  context "Running the consumers in threads" do
    before(:each) do
      QueueMap.mode = :thread
      QueueMap.consumer(:greet).start
    end

    after(:each) do
      QueueMap.consumer(:greet).stop
      QueueMap.mode = nil
    end

    it "runs the consumer using the queue" do
      timing {
        results = ['Bob', 'Jim'].queue_map(:greet)
        results.should == ['Hello, Bob', 'Hello, Jim']
      }.should < 2
    end

    it "runs the :on_timeout proc if result not received within :timeout seconds" do
      timing {
        results = ['Bob', 'Jim'].queue_map(:greet, :timeout => 0.25, :on_timeout => lambda { |r| "No time for you, #{r}" })
        results.should == ['No time for you, Bob', 'No time for you, Jim']
      }.should < 2
    end
  end
end
