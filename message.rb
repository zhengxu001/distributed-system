class Message
  attr_accessor :type, :timeout, :heartbeat, :vote_request, :round_num, :sender

  def initialize(message, sender, value = nil)
    @type = message
    @sender = sender
    @value = value
  end

  def to_json
    {
      type: @type,
      sender: @sender,
      value: @value
    }.to_json
  end
  # def send_message(recipient_port)
  #   Thread.new do
  #     socket = TCPSocket.new('localhost', recipient_port)
  #     socket.puts(self.to_json)
  #     socket.close
  #   end
  # end
end


