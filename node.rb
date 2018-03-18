require 'json'
require 'socket'
require_relative 'message'
require_relative 'membership'
Thread.abort_on_exception = true
MASTER = 'master'
SLAVE = 'slave'
CANDIDATE = 'candidate'

class Node
  attr_accessor :port_num, :state, :server, :membership, :task_queue, :join_group


  def initialize(port, join_group=nil)
    @port_num = port
    @task_queue = Queue.new
    @threads = []
    run

    if not join_group
      @state = MASTER 
      @membership = Membership.new(port)
    end
    if join_group
      @state = SLAVE
      @membership = ask_for_membership(join_group)
    end
    # p "port_num is " + @port_num.to_s
    @join_group = join_group
  end

  def send_message(message, recipient_port, value = nil)
    # a = Thread.new do
      p "recipient_port #{recipient_port}"
      socket = TCPSocket.new('localhost', recipient_port)
      message = Message.new(message, @port_num, value)
      p "start sending message #{message.to_json}"
      socket.puts(message.to_json)
      socket.close
    # end
    # a.join
  end

  def add_new_node(port)
    @membership.slave << port
  end


  def ask_for_membership(recipient_port)
    p("ask_for_membership from: ", recipient_port)
    send_message(:ask_for_membership, recipient_port)
  end

  def get_membership(msg)
    p "get_membership"
    v = JSON.parse msg["value"]
    @membership = Membership.new(v['master'], v['slave'])
    p "got membership"
    p @membership
  end

  def return_membership(msg)
    p "return_membership"
    add_new_node(msg["sender"])
    send_message(:return_membership, msg["sender"], @membership.to_json)
  end

  def respond_to_heartbeat(msg)
    send_message(:acknowledgement, msg["sender"])
  end

  def listen
    server = TCPServer.new(@port_num)
    # loop do
    for i in 0..10000000000
      p "listen: " + @port_num.to_s
      socket = server.accept
      raw_message = socket.gets
      raw_msg = JSON.parse(raw_message)
      @task_queue << raw_msg
      socket.close
      sleep 1; 
    end
  end

  def react
    loop do
      msg = @task_queue.shift
      p 'has task'
      p (msg)
      if msg
        respond_to_heartbeat(msg) if msg["type"] == "heartbeat"
        return_membership(msg) if msg["type"] == "ask_for_membership"
        get_membership(msg) if msg["type"] == "return_membership"
      end
      sleep 1; 
    end
  end

  def send_heartbeats
      for i in 0..10000000000
        if @membership
          p "current membership"
          p @membership
          (@membership.all_nodes - [@port_num]).each do |port|
            send_message(:heartbeat, port)
          end
          sleep 1;
        end
      end
  end

  def show_membership
    global_puts @membership
  end

  def add_new_member(port)
    @membership.slave << port
    sync_membership
  end

  def sync_membership
    @membership.slave.each do |port|
      global_puts "send_membership_to all the ports"
      send_message(:return_membership, port, @membership)
    end
  end

  def ask_for_vote
    
  end

  def give_vote
    
  end

  def run
    t1 = Thread.new { listen }
    t2 = Thread.new { send_heartbeats }
    t3 = Thread.new { react }
    @threads = [t1, t2, t3]
    p "join threads"
    @threads.each(&:join)

    # p("init membership: ")
    # print(@membership)
    # ask_for_membership(join_group) if !@membership
  end

  # def join
    
  # end
end


DEBUG_QUEUE = Queue.new

def global_puts(msg)
  DEBUG_QUEUE.push(msg)
  DEBUG_QUEUE.push(global_state)
end

ARGV.each do|a|
  puts "Argument: #{a}"
end

a = Node.new(ARGV[0], ARGV[1])

# loop do
#   puts DEBUG_QUEUE.shift
# end
# a.show_membership

# b = Node.new(8009, 8008)
# b.run
