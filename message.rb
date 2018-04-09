require 'json'

class Message
  attr_accessor :type, :round_num, :sender, :value

  def initialize(message, sender, round_num, value = nil)
    @type = message
    @sender = sender
    @value = value
    @round_num = round_num
  end

  def to_json
    {
      type: @type,
      sender: @sender,
      round_num: @round_num,
      value: @value
    }.to_json
  end

  def self.timeout_message(type, sender, round_num)
    self.new(type, sender, round_num)
  end

  # def send_message(recipient_port)
  #   Thread.new do
  #     socket = TCPSocket.new('localhost', recipient_port)
  #     socket.puts(self.to_json)
  #     socket.close
  #   end
  # end
end

class Pnode
  attr_accessor :node_num, :port

  def initialize(node_num, port)
    @node_num = node_num
    @port = port
  end

  def to_json
    {
      node_num: @node_num,
      port: @port
    }
  end
end

