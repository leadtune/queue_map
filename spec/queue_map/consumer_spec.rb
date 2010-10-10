require "spec_helper"
describe QueueMap::Consumer do
  describe QueueMap::Consumer::Configurator do
    it "Should configure a Queue-Map consumer" do
      before_proc = lambda { }
      after_proc  = lambda { }
      worker_proc = lambda { }
      consumer = QueueMap::Consumer.new do
        before_fork   &before_proc
        after_fork    &after_proc
        count_workers 5
        worker        &worker_proc
      end
      consumer.count_workers.should == 5
      consumer.before_fork_procs.should == [before_proc]
      consumer.after_fork_procs.should  == [after_proc ]
      consumer.worker_proc.should       == worker_proc
    end
  end
end
