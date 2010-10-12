class QueueMap::Consumer
  attr_accessor :count_workers, :worker_proc, :on_exception_proc
  attr_reader :name, :master_pid
  attr_writer :pid_file, :log_file
  class Configurator
    def initialize(base)
      @base = base
    end

    def before_fork(&block);        @base.before_fork_procs       << block; end
    def after_fork(&block);         @base.after_fork_procs        << block; end
    def between_responses(&block);  @base.between_responses_procs << block; end
    def worker(&block);             @base.worker_proc             =  block; end
    def on_exception(&block);       @base.on_exception_proc       =  block; end
    def count_workers(value);       @base.count_workers           =  value; end
    def pid_file(value);            @base.pid_file                =  value; end
    def log_file(value);            @base.log_file                =  value; end

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
    when :thread
      extend(ThreadStrategy)
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

  def between_responses_procs
    @between_responses_procs ||= []
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

  def run_consumer
    QueueMap.with_bunny do |bunny|
      q = bunny.queue(name.to_s, :durable => false, :auto_delete => true)
      while msg = q.pop
        # STDOUT << "[#{Thread.current[:id]}]"
        return if @shutting_down
        begin
          (sleep 0.05; next) if msg == :queue_empty
          msg = Marshal.load(msg)
          result = worker_proc.call(msg[:input])
          bunny.queue(msg[:response_queue]).publish(Marshal.dump(:result => result, :index => msg[:index]))
          between_responses_procs.each { |p| p.call }
        rescue Exception => e
          if on_exception_proc
            on_exception_proc.call(e)
          else
            logger.error e.message
            logger.error e.backtrace
          end
        end
      end
    end
  end

  module ThreadStrategy
    def start
      @threads = []
      count_workers.times do |c|
        @threads << (Thread.new do
                       begin
                         run_consumer
                       rescue Exception => e
                         logger.error %(#{e}\n#{e.backtrace.join("\n")})
                       end
                     end)
      end
    end

    def stop(graceful = true)
      if graceful
        @shutting_down = true
      else
        @threads.each { |t| t.kill }
      end
    end
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
