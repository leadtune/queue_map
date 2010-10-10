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
    puts "Egad"
  end

  def stop
  end

  def run_consumer
    QueueMap.with_bunny do |bunny|
      q = bunny.queue(name.to_s)
      while msg = q.pop
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

    def stop
      @threads.each { |t| t.kill }
    end
  end
end
