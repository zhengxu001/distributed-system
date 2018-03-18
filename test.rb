x = Thread.new { sleep 0.1; 
	for i in 0..100000
		p i	
	end
}

a = Thread.new { 
	for i in -99999..0
		p i
	end
}  
x.join # Let the threads finish before  
a.join # main thread exits...  