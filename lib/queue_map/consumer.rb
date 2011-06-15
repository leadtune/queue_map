class QueueMap::Consumer
  attr_accessor :count_workers, :worker_proc, :on_exception_proc, :job_timeout
  attr_reader :name, :master_pid
  attr_writer :pid_file, :log_file, :idle_proc
  class Configurator
    def initialize(base)
      @base = base
    end

    def before_fork(&block);        @base.before_fork_procs       << block; end
    def after_fork(&block);         @base.after_fork_procs        << block; end
    def after_response(&block);     @base.after_response_procs    << block; end
    def before_job(&block);         @base.before_job_procs        << block; end
    def worker(&block);             @base.worker_proc             =  block; end
    def on_exception(&block);       @base.on_exception_proc       =  block; end
    def count_workers(value);       @base.count_workers           =  value; end
    def pid_file(value);            @base.pid_file                =  value; end
    def log_file(value);            @base.log_file                =  value; end
    def idle(&block);               @base.idle_proc               =  block; end
    def job_timeout(value);         @base.job_timeout             =  value; end

    def respond_to?(*args)
      super || @base.respond_to?(*args)
    end

    def method_missing(method_name, *args)
      @base.send(method_name, *args)
    end
  end


  def self.new_from_block(name, options = { }, &block)
    consumer = new(name, options)
    Configurator.new(consumer).instance_eval(&block)
    consumer
  end

  def self.from_file(consumer_path, options = { })
    name = File.basename(consumer_path).gsub(/_consumer\.rb$/, '').to_sym
    consumer = new(name, options)
    Configurator.new(consumer).instance_eval(File.read(consumer_path), consumer_path, 1)
    consumer
  end

  def initialize(name, options = { })
    @name = name
    case options[:strategy]
    when :fork
      extend(ForkStrategy)
    when :test
      nil
    else
      raise "Invalid strategy: #{options[:strategy]}"
    end
  end

  def after_fork_procs
    @after_fork_procs ||= []
  end

  def before_fork_procs
    @before_fork_procs ||= []
  end

  def after_response_procs
    @after_response_procs ||= []
  end

  def before_job_procs
    @before_job_procs ||= []
  end
  
  def pid_file
    @pid_file ||= "#{name}_consumer.pid"
  end

  def log_file
    @log_file ||= "#{name}_consumer.log"
  end

  def logger
    @logger ||= Logger.new(log_file)
  end

  def start
    raise RuntimeError, "Called start on Consumer without strategy"
  end

  def stop
    raise RuntimeError, "Called stop on Consumer without strategy"
  end

  def idle_proc
    @idle_proc ||= lambda { sleep 0.05 }
  end

  def run_consumer
    begin
      QueueMap.with_bunny do |bunny|
        q = bunny.queue(name.to_s, :durable => false, :auto_delete => false, :ack => true)
        logger.info "Process #{Process.pid} is listening on #{name.to_s}"
        begin
          msg = q.pop
          (idle_proc.call; next) if msg == :queue_empty
          before_job_procs.each { |p| p.call }
          begin
            Timeout.timeout(job_timeout) do
              msg = Marshal.load(msg)
              result = worker_proc.call(msg[:input])
              bunny.queue(msg[:response_queue]).publish(Marshal.dump(:result => result, :index => msg[:index]))
            end
          ensure
            after_response_procs.each { |p| p.call }
          end
        rescue Qrack::ClientTimeout
        rescue Timeout::Error
          logger.info "Job took longer than #{timeout} seconds to complete. Aborting"
        end while ! @shutting_down
      end
    rescue Exception => e # Bunny gets into a strange state when exceptions are raised, so reconnect to queue server if it happens
      if on_exception_proc
        on_exception_proc.call(e)
      else
        logger.info e.class
        logger.error e.message
        logger.error e.backtrace
      end
      sleep 0.2
    end while ! @shutting_down
    logger.info "Done."
  end

  module ForkStrategy
    class ImaChild < Exception; end

    def start
      @child_pids = []
      @master_pid = Process.pid
      before_fork_procs.each { |p| p.call }
      begin
        (count_workers - 1).times do |c|
          pid = Kernel.fork
          raise ImaChild if pid.nil?
          @child_pids << pid
        end
        $0 = "#{name} queue_map consumer: master"
      rescue ImaChild
        $0 = "#{name} queue_map consumer: child"
        # Parent Child Testing Product was a success! http://is.gd/fXmmZ
      end
      Signal.trap("TERM") { stop(false) }
      Signal.trap("INT") { stop }
      after_fork_procs.each { |p| p.call }
      logger.info "#{Process.pid}: running"
      run_consumer
    end

    def stop(graceful = true)
      $0 = $0 + " [shutting down]"
      @child_pids.each do |pid|
        begin
          Process.kill(graceful ? "INT" : "TERM", pid)
        rescue Errno::ESRCH => e
          logger.error "Unable to signal process #{pid}. Does the process not exist?"
        end
      end

      logger.info "#{Process.pid}: stopping (graceful: #{graceful})"
      if graceful
        Thread.new { sleep job_timeout; stop(false) } # after job_timeout seconds, force shut down
        @shutting_down = true
      else
        begin
          Kernel.exit 0
        ensure
          Kernel.exit! 0
        end
      end
    end
  end
end
