class QueueMap::Consumer
  attr_accessor :count_workers, :worker_proc
  attr_reader :name
  class Configurator
    def initialize(base)
      @base = base
    end

    def before_fork(&block);  @base.before_fork_procs << block; end
    def after_fork(&block);   @base.after_fork_procs  << block; end
    def count_workers(value); @base.count_workers     =  value; end
    def worker(&block);       @base.worker_proc       =  block; end
  end


  def initialize(name, config_file = nil, options = { }, &block)
    @name = name
    configurator = Configurator.new(self)
    if block_given?
      configurator.instance_eval(&block)
    else
      configurator.instance_eval(File.read(config_file), config_file, 1)
    end
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
        return if @shutting_down
        (sleep 0.05; next) if msg == :queue_empty
        msg = Marshal.load(msg)
        result = worker_proc.call(msg[:input])
        bunny.queue(msg[:response_queue]).publish(Marshal.dump(:result => result, :index => msg[:index]))
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
                         puts %(#{e}\n#{e.backtrace.join("\n")})
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
      run_consumer
    end

    def stop(graceful = true)
      @child_pids.each do |pid|
        begin
          Process.kill(graceful ? "INT" : "TERM", pid)
        rescue Errno::ESRCH => e
          puts "Unable to signal process #{pid}. Does the process not exist?"
        end
      end

      puts "#{Process.pid}: stopping (graceful: #{graceful})"
      if graceful
        @shutting_down = true
      else
        exit 0
      end
    end
  end
end
