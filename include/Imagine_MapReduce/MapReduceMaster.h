#ifndef IMAGINE_MAPREDUCE_MASTER_H
#define IMAGINE_MAPREDUCE_MASTER_H

#include "common_macro.h"
#include "common_typename.h"

#include "Imagine_Rpc/Imagine_Rpc.h"

#include <atomic>

namespace Imagine_MapReduce
{

class MapReduceMaster
{
 public:
    class ReducerNode
    {
     public:
        ReducerNode() : ip_(""), port_(""), is_ready_(false), stub_(nullptr)
        {
            reducer_lock_ = new pthread_mutex_t;
            pthread_mutex_init(reducer_lock_, nullptr);
        }

        ReducerNode* SetStub(Imagine_Rpc::Stub* stub)
        {
            stub_ = stub;
            ip_ = stub->GetServerIp();
            port_ = stub->GetServerPort();

            return this;
        }

        ReducerNode* AddFiles(const std::vector<std::string>& files)
        {
            for (size_t i = 0; i < files.size(); i++) {
                files_.push_back(files[i]);
            }

            return this;
        }

        bool IsReady() const { return is_ready_.load(); }

        ReducerNode* UpdateIsReadyStat(bool flag)
        {
            is_ready_.store(flag);

            return this;
         }

        std::string GetIp() const { return ip_; }

        std::string GetPort() const { return port_; }

        ReducerNode* Lock()
        {
            pthread_mutex_lock(reducer_lock_);

            return this;
        }

        ReducerNode* UnLock()
        {
            pthread_mutex_unlock(reducer_lock_);

            return this;
        }

     private:
        std::string ip_;
        std::string port_;
        std::vector<std::string> files_;

        pthread_mutex_t *reducer_lock_;
        std::atomic<bool> is_ready_;
        Imagine_Rpc::Stub* stub_;
    };

 public:
    MapReduceMaster();

    MapReduceMaster(const std::string& profile_name);

    MapReduceMaster(const YAML::Node& config);

    ~MapReduceMaster();

    void Init(const std::string& profile_name);

    void Init(const YAML::Node& config);

    void InitLoop(const YAML::Node& config);

    // 目前只支持单文件处理,因为要区分不同文件则不同Mapper应该对应在不同文件的机器
   MapReduceMaster* MapReduce(const std::vector<std::string> &file_list, const size_t reducer_num = DEFAULT_REDUCER_NUM);

    // 向Reducer发送一个预热信息,注册当前MapReduceMaster,并开启心跳
    const MapReduceMaster* StartReducer(const std::string &reducer_ip, const std::string &reducer_port) const;

    const MapReduceMaster* ConnReducer(size_t split_num, const std::string &file_name, const std::string &split_file_name, const std::string &mapper_ip, const std::string &mapper_port, const std::string &reducer_ip, const std::string &reducer_port) const;

    void loop();

    ReducerNode* FindReducerNode(int idx) const;

    MapReduceMaster* SetTaskFile(const std::vector<std::string> &files);

    size_t GetReducerNum() const;

    Imagine_Rpc::Stub* GenerateNewStub() const;

 private:
    // 配置文件字段
    std::string ip_;
    std::string port_;
    size_t reducer_num_;
    size_t split_size_;
    std::vector<std::string> file_list_;
    bool singleton_log_mode_;
    Logger* logger_;

 private:
    // 用于接收mapper和reducer的task进度信息
    Imagine_Rpc::RpcServer *rpc_server_;                    // Rpc服务器对象
    pthread_t *rpc_server_thread_;                          // 开启Rpc服务主线程
    std::vector<std::string> files_;                        // 需要mapper处理的所有文件的文件名
    std::unordered_map<int, ReducerNode *> reducer_map_;    // reducer对应的ip和port信息
    Imagine_Rpc::Stub* stub_;                               // stub原型
};

} // namespace Imagine_MapReduce

#endif