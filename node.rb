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
  attr_accessor :port_num, :state, :server, :membership, :task_queue, :repliers, :voters, :group_name, :node_num, :committers, :request_queue

  TRANSITIONS = {
    MASTER => {
      timeout: :review_last_heartbeat_round,
      acknowledgement: :log_heartbeat_reply,
      heartbeat: :acknowledgement,
      vote_request: :handle_vote_request,
      ask_for_membership: :return_membership,
      join_request: :confirm_join_request,
      confirmed_request: :check_comitted,
      vote: :already_master
    },
    SLAVE => {
      timeout: :launch_candidacy,
      heartbeat: :acknowledge_heartbeat,
      vote_request: :handle_vote_request,
      return_membership: :update_membership,
      confirm_join_request: :confirmed_request,
      committed: :acknowledge_heartbeat,
      ask_for_membership: :return_membership,
      vote: :already_slave,
      acknowledgement: :already_slave
    },
    CANDIDATE => {
      timeout: :launch_candidacy,
      vote_request: :handle_vote_request,
      vote: :handle_vote,
      confirm_join_request: :confirmed_request,
      heartbeat: :acknowledge_heartbeat,
      ask_for_membership: :return_membership,
      acknowledgement: :already_slave
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


  def initialize(node_num, group_name, port, create_group = false)
    @node_num = node_num
    @port_num = port
    @group_name = group_name
    @task_queue = Queue.new
    @threads = []
    @request_queue = []
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
        p "No Such Group Existed. Creating a New Group"
        @state = MASTER
        @round_num = 0
        master = {"node_num" => node_num, "port_num" => port}
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
    @committers = []
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

  def confirm_join_request(msg)
    if @membership.total_number == 1
      puts "Committed The Join Request".red
      @round_num = @round_num + 1
      @membership = Membership.new(@group_name, @membership.master, [msg.sender])
      send_message(:committed, msg.sender["port_num"], @round_num)
      return
    end
    @committers = []
    puts "Ask Request Commit from Other Members}".red
    @membership.all_nodes.each do |node|
      if node["port_num"] != @port_num
        send_message(:confirm_join_request, node["port_num"], @round_num, msg.sender)
      end
    end
  end

  def confirmed_request(msg)
    puts "Commit the Requst from Master".red
    send_message(:confirmed_request, msg.sender["port_num"], @round_num, msg.value)
  end

  def check_comitted(msg)
    if msg.round_num >= @round_num
      @committers << msg.sender
    end
    if has_majority?(@committers.count + 1.5)
      if msg.value.is_a?(Hash)
        puts "Commited The Join Requests".red
        send_message(:committed, msg.value["port_num"], @round_num)
        @round_num = @round_num + 1
        @membership.slave << msg.value
        @membership = Membership.new(@group_name, @membership.master, @membership.slave)
        @committers = []
      elsif msg.value.is_a?(Array)
        puts "Commited Deleting Faild Nodes".red
        @round_num = @round_num + 1
        @membership = Membership.new(@group_name, @membership.master, msg.value)
        # @repliers = []
        @committers = []
        # send_heartbeats
      end
      # @last_heartbeat = Time.now.to_f
      # @committers = []
    end
  end

  def handle_vote(msg)
    @voters += [msg.sender["port_num"]]
    @last_heartbeat = Time.now.to_f
    if has_majority?(@voters.count)
      @state = MASTER
      slave = @membership.slave.delete_if { |node| node["port_num"] == @port_num}
      master = {"node_num" => @node_num, "port_num" => @port_num}
      @membership = Membership.new(@group_name, master, slave)
      @round_num = @round_num + 1
      puts "#{prefix} Win the Election".red
      @last_heartbeat = Time.now.to_f
      @committers = []
      @repliers = []
      send_heartbeats
    end
  end

  def update_membership(msg)
    # raw_membership = msg.value
    if msg.round_num >= @round_num
      puts "#{prefix} Updating Local Membership".red
      @membership = Membership.new(@group_name, msg.value["master"], msg.value["slave"])
      @round_num = msg.round_num
      acknowledge_heartbeat(msg)
    end
  end

  def return_membership(msg)
    puts "#{prefix} Sync Membership To Node #{msg.sender["port_num"]}".blue
    send_message(:return_membership, msg.sender["port_num"], @round_num, @membership.to_json)
  end

  # def confirm_join_request(msg)
  #   puts "#{prefix} Conifrm Join Request for Node #{msg.sender["node_num"]}".red
  #   send_message(:confirm_join_request, msg.sender["port_num"])
  # end


  def review_last_heartbeat_round(msg)
    ack_count = @repliers.size + 1.5
    if !has_majority?(ack_count)
      launch_candidacy(msg)
    elsif @repliers.size == @membership.slave_nodes.size
      @last_heartbeat = Time.now.to_f
      @repliers = []
      send_heartbeats
    else
      if @membership.total_number == 2
        @membership = Membership.new(@group_name, @membership.master, [])
        @repliers = []
        @committers = []
        @last_heartbeat = Time.now.to_f
        send_heartbeats
        return
      end
      @committers = []
      @membership.all_nodes.each do |node|
        if node["port_num"] != @port_num
          send_message(:confirm_join_request, node["port_num"], @round_num, @repliers)
        end
      end
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
    @state = SLAVE
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
        msg = Message.timeout_message(:timeout, {"node_num" => @node_num, "port_num" => @port_num}, @round_num)
        @last_heartbeat = Time.now.to_f
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
      Time.now.to_f - @last_heartbeat > rand(1...2)*2
    else
      Time.now.to_f - @last_heartbeat > rand(2.5...5)*2
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
    # temp = 0
    # if (count % 1) == 0.5
    #   temp = count - 0.5
    # end

    if (@membership.all_nodes.size%2) == 0
      return count > @membership.all_nodes.size / 2
    else
      return count >= @membership.all_nodes.size / 2 + 1
    end
  end
end


