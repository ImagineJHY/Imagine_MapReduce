#ifndef IMAGINE_MAPREDUCE_INPUTSPLIT_H
#define IMAGINE_MAPREDUCE_INPUTSPLIT_H

#include<string>

namespace Imagine_MapReduce{

class InputSplit
{
public:

    InputSplit(const std::string& file_name_, int offset_, int length_):file_name(file_name_),offset(offset_),length(length_){}

    ~InputSplit(){};

    int GetLength(){return length;}

    void GetLocations();

    void GetLocationInfo();

    std::string GetFileName(){return file_name;}
    
    int GetOffset(){return offset;}

private:
    std::string file_name;//文件名
    int offset;//偏移量(起始位置)
    int length;//split大小
};



}



#endif