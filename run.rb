require 'terminal-table'
require 'socket'
require 'json'
require_relative 'membership'
require_relative 'message'

@server = TCPServer.new(9999)

def listen()
  loop do
    socket = @server.accept
    raw_message = socket.gets
    raw_msg = JSON.parse(raw_message)
    puts (JSON.pretty_generate raw_msg["value"])
    socket.close
    break
    
  end
end

def delete_json(port_num)
  begin
    groups = JSON.parse File.read("group_list")
  rescue
    groups = []
  end
  groups.delete_if { |group| group["master"]["port_num"].to_i == port_num.to_i}
  file = File.open("group_list", "w")
  file.write JSON.pretty_generate(groups)
  file.close
end

def ask_for_membership(group_name, portnum)
  p("ask_for_membership from: ", group_name)
  recipient_port = nil
  GroupList.list.each do |group|
    if group["group_name"] == group_name
      recipient_port = group["master"]["port_num"]
    end
  end
  send_message(:ask_for_membership, recipient_port, portnum)
end

def send_message(message, recipient_port, port_num)
  socket = TCPSocket.new('localhost', recipient_port)
  sender = {"node_num" => "test_client", "port_num" => port_num}
  message = Message.new(message, sender, nil)
  socket.puts(message.to_json)
  socket.close
rescue => e
  p e
  if(e.message.include? "Connection refused")
    @membership.all_nodes = @membership.all_nodes - [recipient_port]
  end
end

loop do
  rows = []
  rows << [1, "Kill One Node"]
  rows << [2, "Kill Multiple Nodes"]
  rows << [3, "Grep Membership of a Group"]
  rows << [4, "List of all the Groups"]
  rows << [5, "Terminate A Group"]
  table = Terminal::Table.new :rows => rows
  puts table
  p "Please Input your Option"
  # p "2. "
  # p "3. Grep Membership of a Group"
  # p "4. List of all the Groups"
  # p "5. List of all the Groups"
  # p "Please Input your Option"
  option = gets.to_i
  case option
  when 1
  	p "Please Enter the Port Number of the Server"
    port_num = gets.to_i
    system "kill $(lsof -t -i:#{port_num})"
    delete_json(port_num)
  when 2
    p "Please Enter the Port Number of the Servers"
    ports = gets.split(" ")
    ports.each do |port|
      delete_json(port.to_i)
      system "kill $(lsof -t -i:#{port.to_i})"
    end
  when 3
  	p "Please Enter the Group Name"
  	# temp_port = 9999
   #  t1 = Thread.new { listen }
   #  t2 = Thread.new { ask_for_membership(gets.gsub("\n", ""), 9999) }
   #  threads = [t1, t2]
   #  threads.each(&:join)
    group_name = gets.gsub("\n", "")
    begin
      groups = JSON.parse File.read("group_list")
    rescue
      groups = []
    end
    terminate_nodes = []
    groups.each do |group|
      if group["group_name"] == group_name
        p group
      end
    end
  when 4
    p "List of all the Groups"
    begin
      groups = JSON.parse File.read("group_list")
    rescue
      groups = []
    end
    p groups
  when 5
    p "Terminate A Group! Please Enter the Group Name:"
    group_name = gets.gsub("\n", "")
    begin
      groups = JSON.parse File.read("group_list")
    rescue
      groups = []
    end
    terminate_nodes = []
    groups.each do |group|
      if group["group_name"] == group_name
        group["all_nodes"].each do |node|
          terminate_nodes << node["port_num"]
        end
      end
    end
    terminate_nodes.each do |port|
      p port
      delete_json(port.to_i)
      system "kill $(lsof -t -i:#{port.to_i})"
    end
  else
      p "No Such Command Supported"
      exit()
  end
end