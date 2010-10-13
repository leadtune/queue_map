# some test objects we pass around
class Surprise
  def initialize(msg)
    @msg = msg
  end
  
  def to_s
    raise @msg
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

