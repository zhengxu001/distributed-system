require 'json'
require 'socket'
require_relative 'message'
require_relative 'membership'
Thread.abort_on_exception = true
MASTER = 'master'
SLAVE = 'slave'
CANDIDATE = 'candidate'

class Node
  attr_accessor :port_num, :state, :server, :membership, :task_queue, :join_group, :repliers, :voters

  TRANSITIONS = {
    MASTER => {
      timeout: :review_last_heartbeat_round,
      acknowledgement: :log_heartbeat_reply,
      # heartbeat: :respond_to_heartbeat,
      heartbeat: :update_membership,
      vote_request: :handle_vote_request,
      ask_for_membership: :return_membership,
      vote: :already_master,
    },
    SLAVE => {
      timeout: :launch_candidacy,
      # heartbeat: :respond_to_heartbeat,
      heartbeat: :update_membership,
      vote_request: :handle_vote_request,
      return_membership: :update_membership,
      vote: :already_slave,
      acknowledgement: :already_slave,
    },
    CANDIDATE => {
      timeout: :launch_candidacy,
      # timeout: :partitioning,
      vote_request: :handle_vote_request,
      vote: :handle_vote,
      # heartbeat: :respond_to_heartbeat,
      heartbeat: :update_membership,
      acknowledgement: :already_slave,
    },
  }

  def already_master(msg)
    p msg
    p "ALREADY MSTER!!!!!!!!!!!!!!!!!!"
  end

  def already_slave(msg)
    p msg
    p "ALREADY MSTER!!!!!!!!!!!!!!!!!!"
  end


  def initialize(port, join_group=nil)
    @port_num = port
    @task_queue = Queue.new
    @threads = []
    @server = TCPServer.new(@port_num)
    if not join_group
      @state = MASTER
      @membership = Membership.new(port)
    end
    if join_group
      @state = SLAVE
      ask_for_membership(join_group)
    end
    @last_heartbeat = Time.now.to_f
    @round_num = 0
    @repliers = []
    @voters = []
  end

  def handle_vote_request(msg)
    p("handle_vote_request msg.round_num: #{msg.round_num}")
    p("handle_vote_request @round_num #{@round_num}")
    if msg.round_num > @round_num
      send_message(:vote, msg.sender, msg.round_num)
      @state = SLAVE
      @last_heartbeat = Time.now.to_f
      @round_num = msg.round_num
    end
  end

  def handle_vote(msg)
    @voters += [msg.sender]
    @last_heartbeat = Time.now.to_f
    if has_majority?(@voters.count)
      @state = MASTER
      @membership.master = @port_num
      @membership.slave = @membership.slave - [@port_num]
      @membership.update
      send_heartbeats
    end
  end

  def update_membership(msg)
    raw_membership = JSON.parse msg.value
    @membership = Membership.new(raw_membership["master"], raw_membership["slave"])
    @round_num = msg.round_num
    respond_to_heartbeat(msg)
  end

  def review_last_heartbeat_round(msg)
    ack_count = @repliers.size + 1
    p("reply is #{@repliers}")
    if !has_majority?(ack_count)
      launch_candidacy(msg)
    else
      @membership.slave = @repliers
      @repliers = []
      @membership.update
      @last_heartbeat = Time.now.to_f
      send_heartbeats
    end
  end

  def launch_candidacy(msg)
    p("launch_candidacy #{@membership.all_nodes}")
    @voters = [@port_num]
    @round_num =  @round_num + 1
    @state = CANDIDATE
    @last_heartbeat = Time.now.to_f
    (@membership.all_nodes - [@port_num]).each do |port|
      send_message(:vote_request, port, @round_num)
    end
  end

  def partitioning(msg)
    p("Start Send Heartbeat to voters #{@voters - [@port_num]}")
    @state = MASTER
    @membership.master = @port_num
    @membership.slave = @voters - [@port_num]
    @membership.update
    @last_heartbeat = Time.now.to_f
    @membership.slave.each do |port|
      send_message(:heartbeat, port, @round_num, @membership.to_json)
    end
  end

  def send_message(message, recipient_port, round_num=@round_num, value=nil)
      p "recipient_port #{recipient_port}"
      socket = TCPSocket.new('localhost', recipient_port)
      message = Message.new(message, @port_num, round_num, value)
      p "start sending message #{message.to_json}"
      socket.puts(message.to_json)
      socket.close
  rescue => e
    p e
    if(e.message.include? "Connection refused")
      @membership.all_nodes = @membership.all_nodes - [recipient_port]
    end
  end

  def log_heartbeat_reply(msg)
    @repliers << msg.sender
    @repliers.uniq!
    # @round_num = msg.round_num
  end

  def add_new_node(port)
    @membership.slave << port
    # @membership.all_nodes = (@membership.slave <<  @membership.master)
    # @membership.total_number = @membership.all_nodes.uniq.size

    # p @membership
    # @membership.slave = @membership.slave.uniq!
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
    p "return_membership to #{msg.sender}"
    send_message(:return_membership, msg.sender, @round_num, @membership.to_json)
    # add_new_node(msg.sender)
  end

  def respond_to_heartbeat(msg)
    if msg.round_num >= @round_num
      @last_heartbeat = Time.now.to_f
      @state = SLAVE
      @round_num = msg.round_num
      send_message(:acknowledgement, msg.sender)
    end
  end

  def listen
    loop do
      socket = @server.accept
      raw_message = socket.gets
      raw_msg = JSON.parse(raw_message)
      @task_queue << Message.new(raw_msg["type"], raw_msg["sender"], raw_msg["round_num"], raw_msg["value"])
      socket.close
    end
  end

  def react
    loop do
      msg = nil
      if timeout?
        msg = Message.timeout_message(:timeout, @port_num, @round_num)
      elsif !@task_queue.empty?
        msg = @task_queue.shift
      end

      if msg
        method_name = TRANSITIONS[@state][msg.type.to_sym]
        p("msg.type is #{msg.type}")
        send(method_name, msg)
      else
        sleep 0.1
      end

    end
  end

  def timeout?
    if @state == MASTER
      Time.now.to_f - @last_heartbeat > rand(0.6...1.2)*2
    # elsif @state == SLAVE
      # Time.now.to_f - @last_heartbeat > (2.2 + rand(0.1...2.1))
    else
      Time.now.to_f - @last_heartbeat > rand(2.4...4.2)*2
      # Time.now.to_f - @last_heartbeat > 8
    end
  end

  def send_heartbeats
    p("Start Send Heartbeat to Slaves #{@membership.slave}")
    (@membership.all_nodes - [@port_num]).each do |port|
      send_message(:heartbeat, port, @round_num, @membership.to_json)
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
      send_message(:return_membership, port, @round_num, @membership)
    end
  end

  def ask_for_vote
    
  end

  def give_vote
    
  end

  def run
    t1 = Thread.new { listen }
    t2 = Thread.new { react }
    @threads = [t1, t2]
    p "join threads"
    @threads.each(&:join)
  end

  def has_majority?(count)
    count >= (@membership.all_nodes.size / 2) + 1
  end
end



# DEBUG_QUEUE = Queue.new

# def global_puts(msg)
#   DEBUG_QUEUE.push(msg)
#   DEBUG_QUEUE.push(global_state)
# end

# ARGV.each do|a|
#   puts "Argument: #{a}"
# end

a = Node.new(ARGV[0], ARGV[1])
a.run



