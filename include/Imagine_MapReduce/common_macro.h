#ifndef IMAGINE_MAPREDUCE_COMMON_MACRO_H
#define IMAGINE_MAPREDUCE_COMMON_MACRO_H

#define DEFAULT_META_SIZE                               16                                      // 一个meta索引的大小
#define DEFAULT_SPILL_SIZE                              0.2                                     // 默认spill触发大小
#define DEFAULT_READ_SPLIT_SIZE                         1024 * 1024 * 128                       // 默认每次读100m
#define DEFAULT_SPLIT_BUFFER_SIZE                       1024 * 1024 * 100                       // 默认spilt缓冲区大小
#define DEFAULT_AVG_LINE_SIZE                           100                                     // 平均一行大小(预读)
#define DEFAULT_MAP_SHUFFLE_NUM                         5                                       // 默认shuffle大小
#define DEFAULT_PARTITION_NUM                           5                                       // 默认分区数目
#define DEFAULT_DISK_MERGE_NUM                          5                                       // Reduce的Copy阶段,磁盘文件开始merge的文件阈值数
#define DEFAULT_MEMORY_MERGE_SIZE                       1024 * 1024 * 100                       // Reduce的Copy阶段,内存空间开始merge的大小
#define DEFAULT_REDUCER_NUM                             DEFAULT_PARTITION_NUM                   // 默认Reducer数目
#define DEFAULT_HEARTBEAT_INTERVAL_TIME                 2.0                                     // 默认心跳发送时间
#define DEFAULT_HEARTBEAT_DELAY_TIME                    0                                       // 默认第一次心跳发送延迟时间

#define INTERNAL_MAP_TASK_SERVICE_NAME                  "Internal_Map_Task_Service"
#define INTERNAL_MAP_TASK_METHOD_NAME                   "Internal_Map_Task_Method"
#define INTERNAL_REDUCE_TASK_SERVICE_NAME               "Internal_Reduce_Task_Service"
#define INTERNAL_REDUCE_TASK_METHOD_NAME                "Internal_Reduce_Task_Method"
#define INTERNAL_HEARTBEAT_SERVICE_NAME                 "Internal_HeartBeat_Service"
#define INTERNAL_HEARTBEAT_METHOD_NAME                  "Internal_HeartBeat_method"
#define INTERNAL_TASK_COMPLETE_SERVICE_NAME             "Internal_Task_Complete_Service"
#define INTERNAL_TASK_COMPLETE_METHOD_NAME              "Internal_Task_Complete_Method"
#define INTERNAL_START_REDUCE_SERVICE_NAME              "Internal_Start_Reduce_Service"
#define INTERNAL_START_REDUCE_METHOD_NAME               "Internal_Start_Reduce_Method"
#define INTERNAL_RETRIEVE_SPLIT_FILE_SERVICE_NAME       "Internal_Retrieve_Split_File_Service"
#define INTERNAL_RETRIEVE_SPLIT_FILE_METHOD_NAME        "Internal_Retrieve_Split_File_Method"

#endif