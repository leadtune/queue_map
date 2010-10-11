require "spec_helper"
describe QueueMap::Consumer do
  describe QueueMap::Consumer::Configurator do
    it "Should configure a Queue-Map consumer" do
      before_proc            = lambda { }
      after_proc             = lambda { }
      worker_proc            = lambda { }
      between_responses_proc = lambda { }
      on_exception_proc      = lambda { }

      consumer = QueueMap::Consumer.new_from_block(:name, :strategy => :test) do
        before_fork       &before_proc
        after_fork        &after_proc
        count_workers     5
        worker            &worker_proc
        between_responses &between_responses_proc
        on_exception      &on_exception_proc
      end

      consumer.count_workers.should           == 5
      consumer.before_fork_procs.should       == [before_proc]
      consumer.after_fork_procs.should        == [after_proc ]
      consumer.worker_proc.should             == worker_proc
      consumer.on_exception_proc.should       == on_exception_proc
      consumer.between_responses_procs.should == [between_responses_proc]
    end
  end
end
