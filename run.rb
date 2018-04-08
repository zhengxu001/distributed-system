require 'socket'
require 'json'
# require_relative 'node'
require_relative 'message'
p "1. Form a Group"
p "2. Join a Exist Group"
p "3. Leave the Group"
p "4. Grep Current Membership"
p "Please Input your Option"

@server = TCPServer.new(9999)

def listen()
  loop do
    socket = @server.accept
    raw_message = socket.gets
    raw_msg = JSON.parse(raw_message)
    p JSON.parse(raw_msg["value"])
    socket.close
  end
end

   def send_message(message, recipient_port, portnum)
      p "recipient_port #{recipient_port}"
      socket = TCPSocket.new('localhost', recipient_port)
      message = Message.new(message, portnum, nil)
      p "here"
      # p "start sending message #{message.to_json}"
      socket.puts(message.to_json)
      socket.close
  rescue => e
    p e
    if(e.message.include? "Connection refused")
      @membership.all_nodes = @membership.all_nodes - [recipient_port]
    end
  end

option = gets.to_i
case option
	when 1
		p "Please Enter the Portnum of the First Server"
		portnum = gets.to_i
		new_node = Node.new(portnum)
		new_node.run
    when 2
    	p "Please Enter the Portnum of the Server"
    	portnum = gets.to_i
    	p "Please Enter a portnum belongs to the Group "
    	master_port = gets.to_i
    	new_node = Node.new(portnum, master_port)
		new_node.run
	when 3
		p "Please Enter the Portnum of the Server"
		system "kill $(lsof -t -i:#{gets.to_i})"
	when 4
		p "Please Enter the Portnum You Want To Connect"
		temp_port = 9999
		# temp_node = Node.new(temp_port)
    	t1 = Thread.new { listen }
    	t2 = Thread.new { send_message(:grep_membership, gets.to_i, 9999) }
    	threads = [t1, t2]
    	threads.each(&:join)
    else
    	p "No Such Command Supported"
  end




# ruby node.rb 8008
