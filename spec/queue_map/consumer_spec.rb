require "spec_helper"
describe QueueMap::Consumer do
  describe QueueMap::Consumer::Configurator do
    it "Should configure a Queue-Map consumer" do
      before_proc            = lambda { }
      after_proc             = lambda { }
      worker_proc            = lambda { }
      after_response_proc    = lambda { }
      before_job_proc        = lambda { }
      on_exception_proc      = lambda { }

      consumer = QueueMap::Consumer.new_from_block(:name, :strategy => :test) do
        before_fork       &before_proc
        after_fork        &after_proc
        count_workers     5
        worker            &worker_proc
        after_response    &after_response_proc
        before_job        &before_job_proc
        on_exception      &on_exception_proc
        job_timeout       1
        pid_file "my.pid"
        log_file "my.log"
      end

      consumer.count_workers.should           == 5
      consumer.before_fork_procs.should       == [before_proc]
      consumer.after_fork_procs.should        == [after_proc ]
      consumer.worker_proc.should             == worker_proc
      consumer.on_exception_proc.should       == on_exception_proc
      consumer.after_response_procs.should    == [after_response_proc]
      consumer.before_job_procs.should        == [before_job_proc]
      consumer.job_timeout.should == 1
      consumer.pid_file.should == "my.pid"
      consumer.log_file.should == "my.log"
    end

    it "allows me to access instance methods of the consumer from within the config scope" do
      consumer = QueueMap::Consumer.new_from_block(:name, :strategy => :test) do
        worker do |payload|
          logger.class.should == Logger
        end
      end
      consumer.worker_proc.call(nil)
    end

  end
end
