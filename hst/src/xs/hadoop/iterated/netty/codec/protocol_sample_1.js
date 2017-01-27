
var decoder = {
	head:['0X68'],           
	fLength:{
		length:2,          
		out:'int()'        
	},  
	message:{
		length:'fLength()',
		out:'string()'     
	},
	
	tail:'0X92'
}