##HDAOOP SIMPLIZE TOOLKIT hadoop mapreduce简化开发包 

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;虽然大数据技术的发展已经将近10个年头了，hadoop技术仍然没有过时，特别是一些低成本，入门级的小项目，使用hadoop还是蛮不错的。而且，也不是每一个公司都有能力招聘和培养自己的spark人才。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;我本人对于hadoop mapreduce是有一些意见的。hadoop mapreduce技术对于开发人员的友好度不高，程序难写，调试困难，对于复杂的业务逻辑远没有spark得心应手。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2016年的春节前接到一个任务，要在一个没有spark的平台实现电力系统的一些统计分析算法，可选的技术只有hadoop mapreduce。受了这个刺激之后产生了一些奇思妙想，然后做了一些试验，并最终形成HST---hadoop simplize toolkit，还真是无心载柳柳成荫啊。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**HST基本优点如下**：<br/>


- 屏蔽了hadoop数据类型，取消了driver，将mapper和reducer转化为transformer和joiner，业务逻辑更接近sql。相当程度的减少了代码量，极大的降低了大数据编程的门槛，让基层程序员通过简单的学习即可掌握大数据的开发。
- 克服了hadoop mapreduce数据源单一的情况，比如在一个job内，input可以同时读文件和来自不同集群的hbase。
- 远程日志系统，让mapper和reducer的日志集中到driver的控制台，极大减轻了并行多进程程序的调试难度。
- 克服了hadoop mapreduce编写业务逻辑时，不容易区分数据来自哪个数据源的困难。接近了spark（或者sql）的水平。
- 天生的多线程执行，即在mapper和reducer端都默认使用多线程来执行业务逻辑。
- 对于多次迭代的任务，相连的两个任务可以建立关联，下一个任务直接引用上一个任务的结果，使多次迭代任务的代码结构变得清晰优美。



&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**基本概念的小变化：**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Source类代替了hadoop Input体系(format，split和reader)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Transformer代替了mapper<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Joiner代替了Reducer<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;去掉了饱受诟病的Driver，改为内置的实现，现在完全不用操心了。<br/>
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;以下逐条说明

1.**基本上屏蔽了hadoop的数据类型，使用纯java类型**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在原生的hadoop mapreduce开发中，使用org.apache.hadoop.io包下的各种hadoop数据类型，比如hadoop的Text类型，算法的编写中一些转换非常不方便。而在HST中一律使用java基本类型，完全屏蔽了hadoop类型体系。
比如在hbase作为source（Input）的时候，再也不用直接使用ImmutableBytesWritable和Result了，HST为你做了自动的转换。<br/>
现在的mapper（改名叫Transformer了）风格是这样的

```
public static class TransformerForHBase0 extends HBaseTransformer<Long>

...
```

现在map方法叫flatmap，看到没，已经帮你自动转成了string和map
```
public void flatMap(String key, Map<String, String> row,
											Collector<Long> collector) 
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;可阅读xs.hadoop.iterated.IteratedUtil类中关于类型自动转换的部分<br/>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;

2.**克服了hadoop mapreduce数据源单一的情况。比如在一个job内，数据源同时读文件和hbase，这在原生的hadoop mapreduce是不可能做到的**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;以前访问hbase，需要使用org.apache.hadoop.hbase.client.Scan和TableMapReduceUtil，现在完全改为与spark相似的方式。
现在的风格是这样的：

```
Configuration conf0 = HBaseConfiguration.create();
conf0.set("hbase.zookeeper.property.clientPort", "2181");
conf0.set("hbase.zookeeper.quorum","172.16.144.132,172.16.144.134,172.16.144.136");
conf0.set(TableInputFormat.INPUT_TABLE,"APPLICATION_JOBS");
conf0.set(TableInputFormat.SCAN_COLUMN_FAMILY,"cf");
conf0.set(TableInputFormat.SCAN_CACHEBLOCKS,"false");
conf0.set(TableInputFormat.SCAN_BATCHSIZE,"20000");
//...其他hbase的Configuration，可以来自不同集群。
IteratedJob<Long> iJob = scheduler.createJob("testJob")
    .from(Source.hBase(conf0), TransformerForHBase0.class)
.from(Source.hBase(conf1), TransformerForHBase1.class)
.from(Source.textFile("file:///home/cdh/0.txt"),Transformer0.class)
.join(JoinerHBase.class)
```
Hadoop中的input,现在完全由source类来代替。通过内置的机制转化为inputformat，inputsplit和reader。在HST的框架下，其实可以很容易的写出诸如Source.dbms(),Source.kafka()以及Source.redis()方法。想想吧，在一个hadoop job中，你终于可以将任意数据源，例如来自不同集群的HBASE和来自数据库的source进行join了，这是多么happy的事情啊！

3.**远程日志系统。让mapper和reducer的日志集中在driver进行显示，极大减轻了了并行多进程程序的调试难度**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;各位都体验过，job fail后到控制台页面，甚至ssh到计算节点去查看日志的痛苦了吧。对，hadoop原生的开发，调试很痛苦的呢！
现在好了，有远程日志系统，可以在调试时将mapper和reducer的日志集中在driver上，错误和各种counter也会自动发送到driver上，并实时显示在你的控制台上。如果在eclipse中调试程序，就可以实现点击console中的错误，直接跳到错误代码行的功能喽！<br/>
ps：有人可能会问，如何在集群外使用eclipse调试一个job，却可以以集群方式运行呢？这里不再赘述了，网上有很多答案的哦

4.**克服了hadoop mapreduce在join上，区分数据来自哪个数据源的困难，接近spark（或者sql）的水平**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在上面给出示例中，大家都看到了，现在的mapper可以绑定input喽！，也就是每个input都有自己独立的mapper。正因为此，现在的input和mapper改名叫Source和Transformer。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;那么，大家又要问了，在mapper中，我已经可以轻松根据不同的数据输入写出不同的mapper了，那reducer中怎么办，spark和sql都是很容易实现的哦？比如看人家sql<br/>

`Select a.id,b.name from A  a,B  b where a.id = b.id`<br/>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;多么轻松愉悦啊!<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在原生hadoop mapreduce中，在reducer中找出哪个数据对应来自哪个input可是一个令人抓狂的问题呢！<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;现在这个问题已经被轻松解决喽！看下面这个joiner，对应原生的reducer
```
public static class Joiner0 extends Joiner<Long, String, String>
...
Reduce方法改名叫join方法，是不是更贴近sql的概念呢？
		public void join(Long key,RowHandler handler,Collector collector) throws Exception{
			 List<Object> row  = handler.getSingleFieldRows(0);//对应索引为0的source
			 List<Object> row2 = handler.getSingleFieldRows(1);//对应第二个定义的source
             ...
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;注意上面两句，可以按照数据源定义的索引来取出来自不同数据源join后的数据了，以后有时间可能会改成按照别名来取出，大家看源码的时候，会发现别名这个部分的接口都写好了，要不你来帮助实现了吧。

5.**天生的多线程执行，即在mapper和reducer端都默认使用多线程来执行业务逻辑**。<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;看看源码吧，HST框架是并发调用flatMap和join方法的，同时又不能改变系统调用reduce方法的顺序(否则hadoop的辛苦排序可就白瞎了)，这可不是一件容易的事呢!<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;看到这里，有的同学说了。你这个HST好是好，但你搞的自动转换类型这个机制可能会把性能拉下来的。这个吗，不得不承认，可能是会有一点影响。但在生产环境做的比对可以证明，影响太小了，基本忽略不计。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;笔者在生产环境做了做了多次试验，mapper改成多线程后性能并未有提高，特别是对一些业务简单的job，增加Transformer中的并发级别效率可能还会下降。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;很多同学喜欢在mapper中做所谓“mapper端的join”。这种方式，相信在HST中通过提高mapper的并发级别后会有更好的表现。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Reducer中的性能相对原生提升的空间还是蛮大的。大部分的mapreduce项目，都是mapper简单而reducer复杂，HST采用并发执行join的方式对提升reducer性能是超好的。

6.**对于多次迭代的任务，相连的两个任务可以建立关联，在流程上的下一个job直接引用上一个job的结果，使多次迭代任务的代码结构变得清晰优美**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;虽然在最后才提到这一点，但这却是我一开始想要写HST原因。多次迭代的任务太麻烦了，上一个任务要写在hdfs做存储，下一个任务再取出使用，麻烦不麻烦。如果都由程序自动完成，岂不美哉！<br/>
在上一个任务里format一下

```
IteratedJob<Long> iJob = scheduler.createJob("testJob")
...//各种source定义
.format("f1","f2")
```
在第二个任务中，直接引用

```
IteratedJob<Long> stage2Job = scheduler.createJob("stage2Job")
.fromPrevious(iJob, Transformer2_0.class);
		//Transformer2_0.class

		public static class Transformer2_0 extends PreviousResultTransformer<Long>
		...
			public void flatMap(Long inputKey, String[] inputValues,Collector<Long> collector) {
			String f1 = getFiledValue(inputValues, "f1");
			String f2 = getFiledValue(inputValues, "f2");
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;看到没，就是这么简单。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在最开始的计划中，我还设计了使用redis队列来缓冲前面job的结果，供后面的job作为输入。这样本来必须严格串行的job可以在一定程度上并发。另外还设计了子任务的并发调度，这都留给以后去实现吧。

7.**便捷的自定义参数传递**。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;有时候，在业务中需要作一些“开关变量”，在运行时动态传入不同的值以实现不同的业务逻辑。这个问题HST框架其实也为你考虑到了。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Driver中的自定义参数，source中的自定义参数都会以内置的方式传到transformer或joiner中去，方便程序员书写业务<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;查看transformer或joiner的源码就会发现：
getSourceParam(name)和getDriverParam(pIndex)方法，在计算节点轻松的得到在driver和source中设置的各层次级别的自定义参数,爽吧!

8.**其他工具**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;HST提供的方便还不止以上这些，比如在工具类中还提供了两行数据（map类型）直接join的方法。这些都留给你自己去发现并实践吧!


####编码指南：

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;首先，你要有一个能用的hdfs文件系统，用来提供输出路经，缓存等功能。

`IteratedUtil.setBaseHdfsUri("hdfs://172.16.144.132:8020/");`<br/>
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;其次，构造一个调度器
`Scheduler scheduler = new Scheduler("testSchedule",args);`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;注意构造器的参数为调度名称和main方法的参数。因为HST最早被设计用来调度多次迭代任务的，所以这里可以用一个算法名称或项目名称。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;和hadoop原生的driver一样，HST也接收main方法的参数，在driver，mapper（transformer）和reducer（joiner）中都可以非常方便的取出这些参数。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;比如在driver中取出通过main入口传进来的参数：<br/>
`Scheduler.getDriverParam(int pIndex)`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;下一步是定义数据源。HST采用source的概念来包装原生的Input(format,split和recordReader)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;基于文件系统的输入：<br/>
`Source.textFile("file:///home/cdh/0.txt")`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;读hdfs的文件
`Source.textFile("hdfs://172.16.144.132:8020/tmp/iterated/testSchedule/10490729023615275_1459916435343/*")`


&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;基于hbase的source是这个样子的
```
Configuration conf0 = HBaseConfiguration.create();
        conf0.set("hbase.zookeeper.property.clientPort", "2181");
        conf0.set("hbase.zookeeper.quorum", "172.16.144.132,172.16.144.134,172.16.144.136");
        conf0.set(TableInputFormat.INPUT_TABLE,"APPLICATION_JOBS");
        conf0.set(TableInputFormat.SCAN_COLUMN_FAMILY,"cf");
        conf0.set(TableInputFormat.SCAN_CACHEBLOCKS,"false");
conf0.set(TableInputFormat.SCAN_BATCHSIZE,"20000");
iJob.from(Source.hBase(conf0), TransformerForHBase0.class)
```

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;正如大家看到的，HST source的设计实际是在模仿spark

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Source的设计还允许传入一些“开关参数”给Transformer
`Source.setParam(String name,String value)`<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;然后Transformer在mapper所在的计算节点中可以很方便的取出。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;然后就是我们的重头戏IteratedJob登场<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;一个IteratedJob对应一个hadoop job<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;首先构造IteratedJob<br/>
`IteratedJob<Long> iJob = scheduler.createJob("testJob")`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;其次添加Source，Transformer和Joiner
```
IteratedJob<Long> iJob = scheduler.createJob("testJob")
					.from(Source.textFile("file:///home/cdh/0.txt"),Transformer0.class)
					.from(Source.textFile("file:///home/cdh/1.txt"),Transformer1.class)
				.join(Joiner.class)
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;和原生hadoop一样，Transformer和Joiner被配置为类名，框架最终会在mapper和reducer中实例化为对象，并传入参数。
     
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;注册job<br/>
`scheduler.arrange(iJob);`
 
 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;运行这个Scheduler，调度所有注册在其中的IteratedJob<br/>
`scheduler.run();`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;最后，别忘了销毁资源，否则你在本地测试的时候，driver进程退不出去。
`IteratedUtil.destoryResource();`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Driver呢，driver哪去了?没有了，已经内置在IteratedJob中，不用你操心了。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;程序的输出：<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;我本人不提倡使用各种outputformat，我一般都是在reducer中直接写hdfs或hbase。
对于示例中的文件输出可以使用IteratedJob.getLastRunPath()来查看job输出路经, 这是由框架在BaseHdfsUri上自动生成的子路经，每次执行时动态随机生成，保证不重复,杜绝hadoop报“输出路经已经存在”错误。
<br/>
<br/>
    **算法编写:**<br/>
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**Transformer和Joiner**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Transformer和Joiner就是用来代替mapper和reducer的，在概念和角色上却是发生了不小的变化，所以也改了名字。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在HST中Transformer是绑定Source的，也就是说，每一个input都有自己的Transformer，可以针对这个input写自己特殊的业务逻辑。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**咱们先看Transformer**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;首先看Transformer的类型定义，注意其泛型参数<br/>
`public abstract class Transformer<K0,V0,K1>`<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;K0,V0,K1分别对应mapper input的key，value类型，k1是output的key类型，output的value类型被内置为map类型，这是由HST框架自动完成的，不用大家操心。

```
public static class Transformer1_0 extends Transformer<Long, String, Long> {
		public Class<Long> getMapOutputKeyClass() {
			return Long.class;
		}
		
		public void flatMap(Long inputKey, String inputValue,Collector<Long> collector) throws Exception{
			String[] values = inputValue.split("<br/>t");
			collector.singleValueRow(Long.parseLong(values[0]), values[1]);
		}
}
```

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;注意getMapOutputKeyClass用于指明mapper output的key的class类型，因为java语言泛型参数在运行时擦除的特点，只好这样处理了，反正也写不错。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;flatMap方法代替了mapper的map方法，作为一个回调方法，参数都已经自动转化为java类型。第二个参数为collector，专门用来收集输出。输出有三种形式<br/>
（1）单值模式<br/>
	 `collector.singleValueRow(key, value);`<br/>
（2）单行模式,收集不同的属性（按照数据库的概念就是“列”）
```
collector = collector.uniqueRow(id);
collector.setField("id2",  id2)
			  .setField("name", name)
			  .setField("sex",  sex)
			  .setField("address",address)
			  .setField("salary", salary);
```
（3）多行模式,可以返回多行（这里借鉴了数据库“行”的概念）,以map表示一行
```
Map<String, Object> row1 = new HashMap();
...
collector.row(key, row1);
collector.row(key, row2);
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;也就是说对输入的一个key value可以有多行，也正因为此这个方法才叫flatMap，在方法名上还是模仿scala。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;注意，在一次flatMap方法的的回调中，这三种模式不能混用。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;transformer其他方法:
```
getSourceParam(String name)     //得到source的自定义参数
getDriverParam(int pIndex)      //得到driver中main方法参数
reportDriver(Exception e)       //实时向driver汇报一个错误，diver会实时将这个错误打印到控制台上，
这样你就可以向调试一个本地程序一样立即定位到出错误的代码行。
reportDriver(String message)    //和上面类似，实时汇报一个日志消息。
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;实际上，我们在项目中，一般都是mapper简单而reducer复杂。HST也赋予了joiner更多的功能，HST并发调用join方法相对于传统reducer的性能有了很大提升。

**Joiner**<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;同样先看Joiner的泛型参数<br/>
`public abstract class Joiner<K1,K2,V2>`<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;其中k1 joiner输入的key类型，也就是mapper输出的key类型，这两个必须一样。K2,V2是输出类型

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Joiner主要需要实现的方法如下
`public abstract void join(K1 key,RowHandler handler,Collector collector)throws Exception;`<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;其中RowHandler用来区分在同一个key上join的数据来自哪一个source，目前是通过source在driver中定义的索引顺序来区分的。<br/>
RowHandler有如下方法:
```
getRows(int sourceIndex)                 //得到多行
getSingleFieldRows(int sourceIndex)      //得到每行单值的多行
getRow(int sourceIndex)                  //得到单行，但每行多值
```

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;注册counter:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;但目前为止，只有joiner可以注册counter。同hadoop原生的counter不同，HST的counter会实时传回driver，在对不同节点的counter求和之后再实时打印到控制台上，而且job结束后会保证打印最终的counter值。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;HST的counter需要在特殊的生命周期回调中注册：
```
public void shouldRegisterTimer() {
			this.registerCounter("插入HBase数量");
｝
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;注意counter名称可以是中文的

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;可以在join方法和union方法中调用counter<br/>
`this.getCounter("插入HBase数量").incrementAndGet();`<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;可以看出HST的counter就是一个AtomicLong

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;和transformer一样，joiner也实现了以下接口
```
getDriverParam(int pIndex)      //得到driver中main方法参数
reportDriver(Exception e)       //实时向driver汇报一个错误，diver会实时将这个错误打印到控制台上，
这样你就可以向调试一个本地程序一样立即定为到出错误的代码行。
reportDriver(String message)    //和上面类似，实时汇报一个日志消息。
```

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;完整的生命周期：<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Transformer和Joiner和完整的生命周期还包括setup和union函数。Union其实就是cleanup，考虑到可能会有其他数据需要被输出的需要，比如，对所在Transformer的输入做一个统计等，所以起名叫union，一些你自己打开的资源，比如jdbc连接,可以在这里释放掉。<br/>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;任务的提交：<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;提交job到集群同原生hadoop mapreduce没有任何区别，一样是”hadoop jar  jar文件 mainClass”

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**请阅读完整的例程**：<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在test.common包下<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;LocalFile_OnlyMapper_tester.java<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;LocalFileTester.java<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;一个调度内包含多个job的例子<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;LocalFileTester2Stage.java<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;HBaseTester.java<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在包test.elec.jobs.collSource有一个在实际项目中应用的稍微复杂一些的例子，里面的一些方法和模式还是值得初学者学习的。<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;所有示例都运行在hadoop2.6及HBASE0.96上

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;最后，千里之行，始于阅读源代码
QQ:2065683883

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;参考：
http://www.blogjava.net/jonenine/archive/2014/12/06/json.html 




