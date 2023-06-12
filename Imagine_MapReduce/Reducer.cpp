// #include"Reducer.h"

// #include"RPC/RpcClient.h"

// using namespace MapReduce;

// template<typename key,typename value>
// Reducer<key,value>::Reducer(int port_, ReduceCallback reduce_)
// {   
//     if(reduce_)reduce=reduce_;
//     else SetDefaultReduceFunction();

//     map_lock=new pthread_mutex_t;
//     if(pthread_mutex_init(map_lock,nullptr)!=0){
//         throw std::exception();
//     }

//     rpc_server=new RpcServer(port_);
//     rpc_server->Callee("Reduce",std::bind(&Reducer::Reduce,this,std::placeholders::_1));
//     rpc_server->Callee("Register",std::bind(&Reducer::Register,this,std::placeholders::_1));
// }

// template<typename key,typename value>
// Reducer<key,value>::~Reducer()
// {
//     delete rpc_server;
//     delete map_lock;
// }

// template<typename key,typename value>
// void Reducer<key,value>::loop()
// {
//     rpc_server->loop();
// }

// template<typename key,typename value>
// std::vector<std::string> Reducer<key,value>::Register(const std::vector<std::string>& input)
// {
//     /*
//     信息格式
//         -1.文件总数
//         -2.文件名列表
//         -3.本机ip
//         -4.本机port
//     */
//     int new_master_file_num=MapReduceUtil::StringToInt(input[0]);
//     std::pair<std::string,std::string> new_master_pair=std::make_pair(input[new_master_file_num+1],input[new_master_file_num+2]);
//     pthread_mutex_lock(map_lock);
//     if(master_map.find(new_master_pair)!=master_map.end()){
//         throw std::exception();//重复注册
//     }
//     MasterNode* new_master=new MasterNode;
//     new_master->file_num=new_master_file_num;
//     for(int i=0;i<new_master_file_num;i++){
//         new_master->files.insert(std::make_pair(input[i+1],-1));
//     }
//     master_map.insert(std::make_pair(new_master_pair,new_master));
//     //rpc_server->SetTimer();
//     pthread_mutex_unlock(map_lock);
// }

// template<typename key,typename value>
// std::vector<std::string> Reducer<key,value>::Reduce(const std::vector<std::string>& input)
// {
//     /*
//     参数说明
//         -1.split_num:文件被分成的split数
//         -2.file_name:split前的文件名
//         -3.split_file_name:当前要获取的split文件名
//         -4.mapper_ip:文件所在的mapper的ip
//         -5.mapper_port:文件所在的mapper的port
//         -6.master_ip:本机ip
//         -7.master_port:本机port
//     */
//     int split_num=MapReduceUtil::StringToInt(input[0]);
//     std::string file_name=input[1];
//     std::string split_name=input[2];
//     std::string mapper_ip=input[3];
//     std::string mapper_port=input[4];
//     std::pair<std::string,std::string> master_pair=std::make_pair(input[5],input[6]);

//     pthread_mutex_lock(map_lock);
//     typename std::unordered_map<std::pair<std::string,std::string>,MasterNode*,HashPair,EqualPair>::iterator it=master_map.find(master_pair);
//     if(it==master_map.end()){
//         throw std::exception();
//     }
//     std::unordered_map<std::string,int>::iterator file_it=it->second->files.find(file_name);
//     if(file_it==it->second->files.end()){
//         it->second->files.insert(std::make_pair(file_name,split_num-1));
//     }else{
//         file_it->second--;
//     }
//     pthread_mutex_unlock(map_lock);

//     RpcClient::Caller("GetFile",)
// }

// template<typename key,typename value>
// bool Reducer<key,value>::SetDefaultReduceFunction(){
//     reduce=MapReduceUtil::DefaultReduceFunction;
//     return true;
// }