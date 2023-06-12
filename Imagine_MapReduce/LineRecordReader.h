#ifndef IMAGINE_MAPREDUCE_LINERECORDREADER_H
#define IMAGINE_MAPREDUCE_LINERECORDREADER_H

#include<unistd.h>
#include<fcntl.h>

#include"RecordReader.h"

namespace Imagine_MapReduce{

// template<typename key,typename value>
class LineRecordReader : public RecordReader<int,std::string>
{

public:

    LineRecordReader(InputSplit* split_=nullptr, int split_id_=0);

    ~LineRecordReader(){};

    bool NextKeyValue();

    int GetCurrentKey();

    std::string GetCurrentValue();

    double GetProgress();//获取当前运行进展

    void Close();//关闭RecordReader

    std::shared_ptr<RecordReader<int,std::string>> CreateRecordReader(InputSplit* split_, int split_id_);

    bool ReadLine();

protected:
    int offset;
    std::string line_text;

};

// template<typename key,typename value>
LineRecordReader::LineRecordReader(InputSplit* split_, int split_id_):RecordReader(split_,split_id_)
{
    if(split){
        offset=split->GetOffset();
        if(offset){
            ReadLine();
        }
    }
}

// template<typename key,typename value>
bool LineRecordReader::NextKeyValue()
{
    offset+=line_text.size();//下一句的起始位置
    if(offset>=split->GetLength()+split->GetOffset())return false;//lenth+split是下一段的起始位置
    line_text.clear();
    return ReadLine();
}

// template<typename key,typename value>
int LineRecordReader::GetCurrentKey()
{
    return offset;
}

// template<typename key,typename value>
std::string LineRecordReader::GetCurrentValue()
{
    return line_text.substr(0,line_text.size()-2);
}

// template<typename key,typename value>
double LineRecordReader::GetProgress()
{
    return (offset-split->GetOffset())*1.0/split->GetLength();
}

// template<typename key,typename value>
void LineRecordReader::Close()
{

}

std::shared_ptr<RecordReader<int,std::string>> LineRecordReader::CreateRecordReader(InputSplit* split_, int split_id_)
{
    return std::make_shared<LineRecordReader>(split_,split_id_);
}

bool LineRecordReader::ReadLine()
{
    int fd=open(&split->GetFileName()[0],O_RDWR);
    lseek(fd,offset,SEEK_SET);
    bool flag=false;
    while(1){
        char c;
        int ret=read(fd,&c,1);
        if(ret){
            line_text.push_back(c);
            if(flag&&c=='\n'){
                break;
            }
            if(c=='\r')flag=true;
            else flag=false;
        }else{//读完了,文件不以\r\n结尾
            line_text+="\r\n";
            break;
        }
    }

    close(fd);
    return true;
}


}


#endif