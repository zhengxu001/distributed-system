require 'colorize'
require 'json'
require 'socket'
require_relative 'message'
require_relative 'membership'

Thread.abort_on_exception = true
MASTER = 'master'
SLAVE = 'slave'
CANDIDATE = 'candidate'

class Node
  attr_accessor :port_num, :state, :server, :membership, :task_queue, :repliers, :voters, :group_name, :node_num

  TRANSITIONS = {
    MASTER => {
      timeout: :review_last_heartbeat_round,
      acknowledgement: :log_heartbeat_reply,
      heartbeat: :acknowledgement,
      vote_request: :handle_vote_request,
      ask_for_membership: :return_membership,
      join_request: :confirm_join_request,
      vote: :already_master,
      grep_membership: :response_membership
    },
    SLAVE => {
      timeout: :launch_candidacy,
      heartbeat: :acknowledge_heartbeat,
      vote_request: :handle_vote_request,
      return_membership: :update_membership,
      confirm_join_request: :acknowledge_heartbeat,
      ask_for_membership: :return_membership,
      vote: :already_slave,
      acknowledgement: :already_slave,
      grep_membership: :response_membership
    },
    CANDIDATE => {
      timeout: :launch_candidacy,
      vote_request: :handle_vote_request,
      vote: :handle_vote,
      heartbeat: :acknowledge_heartbeat,
      ask_for_membership: :return_membership,
      acknowledgement: :already_slave,
      grep_membership: :response_membership
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


  def initialize(node_num, group_name, port, create_group = true)
    @node_num = node_num
    @port_num = port
    @group_name = group_name
    @task_queue = Queue.new
    @threads = []
    @server = TCPServer.new(@port_num)
    if create_group == "true"
      @state = MASTER
      @round_num = 0
      if GroupList.list_name.include? group_name
        Membership.delete_gruop(group_name)
      end
      master = {"node_num" => node_num, "port_num" => port}
      @membership = Membership.new(group_name, master)
    else
      if !GroupList.list_name.include? group_name
        p "No such group existed. Going to create a New Group"
        @state = MASTER
        master = Pnode.new(node_num, port)
        @round_num = 0
        @membership = Membership.new(group_name, master)
      else
        @round_num = -1
        @state = SLAVE
        join_request(group_name)
      end
    end
    @last_heartbeat = Time.now.to_f
    @repliers = []
    @voters = []
  end

  def send_heartbeats
    puts "#{prefix} Send Heartbeat To Slaves".blue
    @membership.all_nodes.each do |node|
      if node["port_num"] != @port_num
        send_message(:heartbeat, node["port_num"], @round_num)
      end
    end
  end

  def handle_vote_request(msg)
    if msg.round_num > @round_num
      send_message(:vote, msg.sender["port_num"], msg.round_num)
      @state = SLAVE
      @last_heartbeat = Time.now.to_f
      @round_num = msg.round_num
      puts "#{prefix} Vote For #{msg.sender["port_num"]}".blue
    end
  end

  def handle_vote(msg)
    @voters += [msg.sender["port_num"]]
    @last_heartbeat = Time.now.to_f
    if has_majority?(@voters.count)
      @state = MASTER
      slave = @membership.all_nodes.delete_if { |node| node["port_num"] == @port_num}
      master = {"node_num" => @node_num, "port_num" => @port_num}
      @membership = Membership.new(@group_name, master, slave)
      puts "#{prefix} Win the Election".red
      send_heartbeats
    end
  end

  def update_membership(msg)
    raw_membership = msg.value
    if msg.round_num >= @round_num
      puts "#{prefix} Updating Local Membership".blue
      @membership = Membership.new(@group_name, raw_membership["master"], raw_membership["slave"])
      @round_num = msg.round_num
      # acknowledge_heartbeat(msg)
    end
  end

  def return_membership(msg)
    puts "#{prefix} Sync Membership To Node #{msg.sender["port_num"]}".blue
    send_message(:return_membership, msg.sender["port_num"], @round_num, @membership.to_json)
  end

  def confirm_join_request(msg)
    puts "#{prefix} Conifrm Join Request for Node #{msg.sender["node_num"]}".red
    send_message(:confirm_join_request, msg.sender["port_num"])
  end


  def review_last_heartbeat_round(msg)
    ack_count = @repliers.size + 1.5
    if !has_majority?(ack_count)
      launch_candidacy(msg)
    elsif @repliers.size == @membership.slave_nodes.size
      @last_heartbeat = Time.now.to_f
      @repliers = []
      send_heartbeats
    else
      @membership = Membership.new(@group_name, @membership.master, @repliers)
      @repliers = []
      @round_num = @round_num + 1
      @last_heartbeat = Time.now.to_f
      send_heartbeats
    end
  end

  def launch_candidacy(msg)
    puts "#{prefix} Becaming Candidate".red
    @voters = [@port_num]
    @round_num =  @round_num + 1
    @state = CANDIDATE
    @last_heartbeat = Time.now.to_f
    @membership.all_nodes.each do |node|
      if node["port_num"] != @port_num
        send_message(:vote_request, node["port_num"], @round_num)
      end
    end
  end

  def send_message(message, recipient_port, round_num=@round_num, value=nil)
    socket = TCPSocket.new('localhost', recipient_port)
    sender = {"node_num" => @node_num, "port_num" => @port_num}
    message = Message.new(message, sender, round_num, value)
    socket.puts(message.to_json)
    socket.close
  rescue => e
    puts e.to_s.red
    if(e.message.include? "Connection refused")
      @membership.all_nodes = @membership.all_nodes - [recipient_port]
    end
  end

  def log_heartbeat_reply(msg)
    @repliers << msg.sender
    @repliers.uniq!
    if msg.round_num < @round_num
      return_membership(msg)
    end
  end

  def join_request(group_name)
    puts("#{prefix} Request Join To: #{group_name}".red)
    recipient_port = nil
    GroupList.list.each do |group|
      if group["group_name"] == group_name
        recipient_port = group["master"]["port_num"]
      end
    end
    send_message(:join_request, recipient_port)
  end

  def ask_for_membership(group_name)
    puts("New Node Ask For Membership From: #{group_name}".red)
    recipient_port = nil
    GroupList.list.each do |group|
      if group["group_name"] == group_name
        recipient_port = group["master"]["port_num"]
      end
    end
    send_message(:ask_for_membership, recipient_port)
  end

  def acknowledge_heartbeat(msg)
    puts "#{prefix} Acknowledgement To Master".blue
    @last_heartbeat = Time.now.to_f
    send_message(:acknowledgement, msg.sender["port_num"])
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
        msg = Message.timeout_message(:timeout, {"node_num" => "node1", "port_num" => @port_num}, @round_num)
      elsif !@task_queue.empty?
        msg = @task_queue.shift
      end

      if msg
        method_name = TRANSITIONS[@state][msg.type.to_sym]
        puts  "#{prefix}: Processing #{msg.type} from #{msg.sender["node_num"]}".blue
        send(method_name, msg)
      else
        sleep 0.1
      end

    end
  end

  def prefix
    "[#{@state}, #{@round_num}, #{@node_num}, #{@group_name}]"
  end

  def timeout?
    if @state == MASTER
      Time.now.to_f - @last_heartbeat > rand(0.6...1.2)*2
    else
      Time.now.to_f - @last_heartbeat > rand(2.4...4.2)*2
    end
  end

  def run
    t1 = Thread.new { listen }
    t2 = Thread.new { react }
    @threads = [t1, t2]
    @threads.each(&:join)
  end

  def new_threads
    t1 = Thread.new { listen }
    t2 = Thread.new { react }
    return [t1, t2]
  end

  def has_majority?(count)
    if @membership.all_nodes.size == 2 && count >=1
      return true
    end
    count >= (@membership.all_nodes.size / 2) + 0.5
  end
end


