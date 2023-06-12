# Readme

## 简介

Imagine_MapReduce参考Hadoop中MapReduce的设计思路进行了框架设计，主要功能有：

- Mapper定时向Master发送任务进度信息
- Map阶段的split过程，recordread过程，map接口，Collect过程，spill过程，shuffle过程
- Reduce阶段的Copy过程，merge过程，Sort过程，reduce接口
- Mapper与Master的心跳检测与进度跟踪

## 快速上手

- 使用说明

  - Mapper端

    添加头文件Mapper.h，使用namespace Imagine_MapReduce即可快速使用

  - Reducer端

    添加头文件Reducer.h，使用namespace Imagine_MapReduce即可快速使用

  - Master端

    添加头文件MapReduceMaster.h，使用namespace Imagine_MapReduce即可快速使用

- 操作系统：Linux

- 依赖：

  - Imagine_Rpc
  - Imagine_ZooKeeper
  - Imagine_Muduo
  - Linux下线程库pthread

- 启动：

  - Mapper端

    调用Mapper::loop()函数即可启动

  - Reducer端

    调用Reducer::loop()函数即可启动
  
  - RpcZooKeeper端
  
    见Rpc相关文档
  
  - Master端
  
    调用MapReduceMaster::loop()函数，然后调用MapReduce函数开启MapReduce过程


## 说明

- Mapper相关接口

  ```cpp
  //构造函数
  Mapper::Mapper(const std::string& ip_, const std::string& port_, RecordReader<reader_key,reader_value>* record_reader_=nullptr, MAP map_=nullptr, Partitioner<key>* partitioner_=nullptr, OutputFormat<key,value>* output_format_=nullptr, MAPTIMER timer_callback_=nullptr, const std::string& keeper_ip_="", const std::string& keeper_port_="");
  /*
  -参数
  	-ip_:服务器Mapper的Ip
  	-port_:你想要服务器运行的端口
  	-record_reader_:提供数据读取方法的类,框架提供了一个默认实现LineRecordReader(将一行数据按kv读入),若要实现自己方法,需要继承Record_Reader
  	-map_:用户的map方法
  	-partitioner_:为map_函数的输出kv提供分区方法,框架提供了一个默认实现StringRecordReader(对std::string类型的输出key进行哈希分区),若要实现自己方法,需要继承Partitioner
  	-output_format_:为map_函数的输出kv提供持久化格式方法,框架提供了一个默认实现TextOutputFormat(对std::string类型的key以及int类型的value进行持久化),若要实现自己方法,需要继承OutputFormat
  	-timer_callback_:Mapper与Master建立连接后的心跳检测函数,框架提供了默认实现(可发送任务进度信息),传nullptr即可
  	-keeper_ip_:服务器RpcZooKeeper的port
  	-keeper_port_:服务器eper的port
  注:
  -框架的StringRecordReader目前将“ ”作为kv的分隔符,将"\r\n"作为line之间的分隔符,暂未作异常处理,因此kv中最好不要出现这三个字符
  -框架的OutputFormat是将map_得到的kv对转化为char[]类型,TextOutputFormat因为一些特殊原因暂未将value值进行持久化
  */
  ```

- Reducer相关接口

  ```cpp
  //构造函数
  Reducer::Reducer(const std::string& ip_, const std::string& port_, const std::string& keeper_ip_="", const std::string& keeper_port_="", ReduceCallback reduce_=nullptr);
  /*
  -参数
  	-ip_:服务器Reducer的Ip
  	-port_:你想要服务器运行的端口
      -keeper_ip_:服务器RpcZooKeeper的port
  	-keeper_port_:服务器eper的port
  	-reducer_:暂时废弃的接口
  注:当前reducer阶段做的任务就是把所有的shuffle文件merge到一个磁盘文件
  */
  ```
  
- MapReduceMaster相关接口

  ```cpp
  //构造函数
  MapReduceMaster::MapReduceMaster(const std::string& ip_, const std::string& port_, const std::string& keeper_ip_, const std::string& keeper_port_, const int reducer_num_=DEFAULT_REDUCER_NUM);
  /*
  -参数
  	-ip_:服务器Reducer的Ip
  	-port_:你想要服务器运行的端口
      -keeper_ip_:服务器RpcZooKeeper的port
  	-keeper_port_:服务器eper的port
  	-reducer_num_:你期望参与运行的reducer数目(默认为5)
  */
  
  bool MapReduceMaster::MapReduce(const std::vector<std::string>& file_list, const int reducer_num=DEFAULT_REDUCER_NUM, const int split_size=DEFAULT_READ_SPLIT_SIZE);
  /*
  -参数
  	-file_list:需要处理的文件列表
  	-reducer_num:废弃参数,实际执行以构造函数中reducer_num_大小为准
  	-split_size:文件split大小
  -返回值
  	-true:成功
  	-false:失败
  注:考虑到文件和Mapper地址应在一起,并且Master需要通过RpcZooKeeper获取Mapper地址,暂时没有设计如何保证获取的Mapper本地一定有文件,因此当前只支持处理单Mapper处理单文件,但是支持对文件进行分片(Split)
  */
  ```
  
  