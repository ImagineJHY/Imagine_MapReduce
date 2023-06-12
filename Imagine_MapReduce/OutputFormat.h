#ifndef IMAGINE_MAPREDUCE_OUTPUTFORMAT_H
#define IMAGINE_MAPREDUCE_OUTPUTFORMAT_H

namespace Imagine_MapReduce{

template<typename key,typename value>
class OutputFormat
{

public:

    OutputFormat(){};

    virtual ~OutputFormat(){};

    virtual std::pair<char*,char*> ToString(std::pair<key,value> content)=0;
};


}


#endif