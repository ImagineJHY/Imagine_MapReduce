#include "MapReduceUtil.h"
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <sys/stat.h>

using namespace Imagine_MapReduce;

void MapReduceUtil::DefaultMapFunction(const std::string &read_split)
{
    int idx = read_split.find_first_of('\r') + 2;
    std::unordered_map<std::string, int> kv_map;
    // kv_map.insert(std::make_pair("sdfasdf",3));
    // DefaultMapFunctionHashHandler(read_split,kv_map);
    std::vector<int> shuffle_fd;
    for (int i = 0; i < DEFAULT_MAP_SHUFFLE_NUM; i++) {
        shuffle_fd.push_back(open(&("shuffle" + MapReduceUtil::IntToString(i) + ".txt")[0], O_CREAT | O_RDWR, 0777));
    }

    auto hash_func = kv_map.hash_function();
    // for(std::unordered_map<std::string,int>::iterator it=kv_map.begin();it!=kv_map.end();it++){
    //     int hash_fd=hash_func(it->first)%5;
    //     write(shuffle_fd[hash_fd],&it->first[0],it->first.size());
    //     write(shuffle_fd[hash_fd],"\r\n",2);
    //     //printf("string %s occur %d times!\n",&(it->first[0]),it->second);
    // }
    for (int i = idx + 1; i < read_split.size(); i++) {
        if (read_split[i] == '\r' && read_split[i + 1] == '\n') {
            std::string temp_string = read_split.substr(idx, i - idx);
            int hash_fd = hash_func(temp_string) % 5;
            int ret = write(shuffle_fd[hash_fd], &temp_string[0], temp_string.size());
            // printf("ret is %d\n",ret);
            write(shuffle_fd[hash_fd], "\r\n", 2);
            idx = i + 2;
            i++;
        }
    }

    for (int i = 0; i < DEFAULT_MAP_SHUFFLE_NUM; i++) {
        close(shuffle_fd[i]);
    }
}

void MapReduceUtil::DefaultMapFunctionHashHandler(const std::string &input, std::unordered_map<std::string, int> &kv_map)
{
    int idx = 0;
    for (int i = 0; i < input.size(); i++) {
        if (input[i] == '\r' && input[i + 1] == '\n') {
            std::unordered_map<std::string, int>::iterator it = kv_map.find(input.substr(idx, i - idx));
            // printf("get string %s\n",&(input.substr(idx,i-idx))[0]);
            if (it == kv_map.end()) {
                kv_map.insert(std::make_pair(input.substr(idx, i - idx), 1));
            } else {
                it->second++;
            }
            idx = i + 2;
            i++;
        }
    }
}

void MapReduceUtil::DefaultReduceFunction(const std::string &input)
{
    int file_fd = open(&input[0], O_RDWR);
    if (file_fd == -1) {
        perror("open");
    }

    fcntl(file_fd, O_NONBLOCK);
    std::string tail_string;
    std::unordered_map<std::string, int> kv_map;
    while (1) {
        bool flag = true;
        std::string read_string = tail_string;
        read_string.resize(tail_string.size() + DEFAULT_READ_SPLIT_SIZE);
        int ret = read(file_fd, &read_string[tail_string.size()], DEFAULT_READ_SPLIT_SIZE);
        if (ret == -1) {
            perror("read");
        }
        if (!ret) {
            break;
        }
        read_string.resize(tail_string.size() + ret);
        tail_string.resize(0);
        for (int i = read_string.size() - 1; i >= 0; i--) {
            if (i > 0 && read_string[i] == '\n' && read_string[i - 1] == '\r') {
                if (i == read_string.size() - 1) {
                    break; // 刚好读完一句
                }
                tail_string = read_string.substr(i + 1, read_string.size() - i - 1);
                read_string.resize(i + 1);
                break;
            }
            if (i == 0) { // 找不到
                flag = false;
                tail_string = read_string;
                break;
            }
        }
        if (flag) {
            DefaultReduceFunctionHashHandler(read_string, kv_map);
        }
    }
}

void MapReduceUtil::DefaultReduceFunctionHashHandler(const std::string &input, std::unordered_map<std::string, int> &kv_map)
{
    int idx = 0;
    int split_idx; // 空格位置
    for (int i = 0; i < input.size(); i++) {
        if (input[i] == ' ') {
            split_idx = i;
        } else if (input[i] == '\r' && input[i + 1] == '\n') {
            std::unordered_map<std::string, int>::iterator it = kv_map.find(input.substr(idx, split_idx - idx));
            if (it == kv_map.end()) {
                kv_map.insert(std::make_pair(input.substr(idx, i - idx), StringToInt(input.substr(split_idx + 1, i - split_idx - 1))));
            } else {
                it->second += StringToInt(input.substr(split_idx + 1, i - split_idx - 1));
            }
            idx = i + 2;
            i++;
        }
    }
}

std::vector<InputSplit *> MapReduceUtil::DefaultReadSplitFunction(const std::string &file_name, int split_size)
{
    std::vector<InputSplit *> splits;
    struct stat statbuf;
    int offset = 0;
    int ret = stat(&file_name[0], &statbuf);
    if (ret == -1) {
        perror("stat");
    }
    while (statbuf.st_size >= split_size) {
        splits.push_back(new InputSplit(file_name, offset, split_size));
        statbuf.st_size -= split_size;
        offset += split_size;
    }
    if (statbuf.st_size) {
        splits.push_back(new InputSplit(file_name, offset, statbuf.st_size));
    }

    // for(int i=0;i<splits.size();i++){
    //     printf("%d\n",splits[i]->GetLength());
    // }

    return splits;
}

int MapReduceUtil::StringToInt(const std::string &input)
{
    int output = 0;
    int size = input.size();
    for (int i = 0; i < size; i++) {
        output = output * 10 + input[i] - '0';
    }

    return output;
}

std::string MapReduceUtil::IntToString(int input)
{
    std::string temp_output;
    std::string output;

    if (!input) {
        return "0";
    }
    while (input) {
        temp_output.push_back(input % 10 + '0');
        input /= 10;
    }

    for (int i = temp_output.size() - 1; i >= 0; i--) {
        output.push_back(temp_output[i]);
    }

    return output;
}

std::string MapReduceUtil::DoubleToString(double input)
{
    int time = 0;
    int integer_part = (int)input;
    std::string output = IntToString(integer_part);
    output.push_back('.');
    input -= integer_part;
    while (input && time < 2) {
        time++;
        int temp_value = input * 10;
        output += IntToString(temp_value);
        input = input * 10 - temp_value;
    }

    if (!time) {
        output.push_back('0');
    }

    return output;
}

bool MapReduceUtil::ReadKVReaderFromMemory(const std::string &content, int &idx, std::string &key, std::string &value) // 从spill文件读取kv对
{
    // 假设空格是key和value的分隔符,且每个kv以\r\n结尾,且kv中不包含这三个字符
    if (idx >= content.size()) {
        return false;
    }
    int start_idx = idx;
    while (content[++idx] != ' ');
    key = content.substr(start_idx, idx - start_idx);

    bool flag = false;
    start_idx = idx + 1;
    while (1) {
        if (content[idx] == '\n' && flag) {
            value = content.substr(start_idx, idx - start_idx - 1);
            idx++; // 下一处起点位置
            return true;
        } if (content[idx] == '\r') {
            flag = true;
        } else {
            flag = false;
        }
        idx++;
    }
}

bool MapReduceUtil::ReadKVReaderFromDisk(const int fd, std::string &key, std::string &value)
{
    // 假设空格是key和value的分隔符,且每个kv以\r\n结尾,且kv中不包含这三个字符
    bool flag = false;
    char c;
    int ret = read(fd, &c, 1);
    // 文件读空
    if (!ret) {
        return false;
    }

    key.push_back(c);
    while (1) {
        // printf("here\n");
        read(fd, &c, 1);
        if (c == ' ') {
            break;
        }
        key.push_back(c);
    }
    while (1) {
        read(fd, &c, 1);
        value.push_back(c);
        if (c == '\n' && flag) {
            value.pop_back();
            value.pop_back();
            return true;
        }
        if (c == '\r') {
            flag = true;
        } else {
            flag = false;
        }
    }
}

bool MapReduceUtil::WriteKVReaderToDisk(const int fd, const KVReader *const kv_reader)
{
    char c = ' ';
    char cc[3] = "\r\n";
    write(fd, &kv_reader->reader_key_[0], kv_reader->reader_key_.size());
    write(fd, &c, 1);
    write(fd, &kv_reader->reader_value_[0], kv_reader->reader_value_.size());
    write(fd, cc, 2);

    return true;
}

bool MapReduceUtil::MergeKVReaderFromDisk(const int *const fds, const int fd_num, const std::string &file_name)
{
    std::priority_queue<KVReader *, std::vector<KVReader *>, KVReaderCmp> heap;
    int fd = open(&file_name[0], O_CREAT | O_RDWR, 0777);
    for (int i = 0; i < fd_num; i++) {
        std::string key;
        std::string value;
        if (ReadKVReaderFromDisk(fds[i], key, value)) {
            heap.push(new KVReader(key, value, fds[i]));
        }
    }
    while (heap.size()) {
        KVReader *next = heap.top();
        heap.pop();
        std::string key;
        std::string value;
        if (ReadKVReaderFromDisk(next->reader_idx_, key, value)) {
            heap.push(new KVReader(key, value, next->reader_idx_));
        }
        WriteKVReaderToDisk(fd, next);

        delete next;
    }

    close(fd);

    return true;
}