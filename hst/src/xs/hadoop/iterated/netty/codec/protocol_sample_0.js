
var decoder = {
	head:['0X68','0X67'],  //称作一项,项有2种类型,值型和复杂性
	                       //值型的要转换成复杂性,自动添加length out validate
	fLength:{
		length:2,          //复杂型的定义length out validate
		out:'int()'        //如果不定义out,就用byte[]的方式返回
	},
	
	
	message:{
		length:'fLength()',//在上一个item解析之后执行
		out:'string()'     //在本item的length到了在执行
	},
	fCheck:{
		length:1,
		validate:'check_sum()' //在本item的length到了在执行
	},
	tail:['0X92',109]
}