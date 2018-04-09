require 'thread'
load 'node.rb'
p "How Many Group Deos This Nodes Belong To?:"
threads =[]
a = gets.to_i - 1
MUTEX = Mutex.new

def create_server(a,b,c,d)
  server = Node.new(a, b, c, d);
  server.run
end

for i in 0..a
	p "Please Enter the first Role:"
	a, b, c, d = gets.split(" ")
	threads << Thread.new do
	  create_server(a, b, c, d)
	end
end
p threads
threads.each(&:join)
