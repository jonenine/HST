
var decoder = {
	head:['0XA1'],  
	                       
	messageId:{
		length:4,          
		out:'int()'        
	},
	
	frameNums:{
		length:2,
		out:'int()'
	},
	
	frameIndex:{
		length:2,
		out:'int()'
	},
	
	frameLength:{
		length:2,         
		out:'int()'        
	},
	
	overload:{
		length:'frameLength()',
		out:'byte()'     
	},
	
	messageCheck:{
		length:8,
		validate:'adler32()'
	},
	
	tail:['0XA2']
}

