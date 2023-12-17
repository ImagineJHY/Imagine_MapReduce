#include "Imagine_MapReduce/TextOutputFormat.h"

namespace Imagine_MapReduce
{

TextOutputFormat::TextOutputFormat()
{
}

TextOutputFormat::~TextOutputFormat()
{
}

std::pair<char *, char *> TextOutputFormat::ToString(const std::pair<std::string, int>& content) const
{
    char *key = new char[content.first.size() + 1];
    char *value = new char[1];

    char c = '\0';
    memcpy(key, content.first.c_str(), content.first.size());
    memcpy(key + content.first.size(), &c, 1);
    memcpy(value, &c, 1);

    return std::make_pair(key, value);
}

} // namespace Imagine_MapReduce
