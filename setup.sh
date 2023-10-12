#!/bin/bash
current_dir=$(pwd)
# export PATH=$PATH:${current_dir}/thirdparty/Imagine_System/worker/Imagine_Muduo/include/
# export PATH=$PATH:${current_dir}/thirdparty/Imagine_System/worker/Imagine_ZooKeeper/include
# export PATH=$PATH:${current_dir}/thirdparty/Imagine_System/worker/Imagine_Rpc/include
# export PATH=$PATH:${current_dir}/thirdparty/Imagine_System/worker/Imagine_MapReduce/include

export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:${current_dir}/thirdparty/Imagine_System/worker/Imagine_Muduo/include
export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:${current_dir}/thirdparty/Imagine_System/worker/Imagine_ZooKeeper/include
export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:${current_dir}/thirdparty/Imagine_System/worker/Imagine_Rpc/include
export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:${current_dir}/thirdparty/Imagine_System/worker/Imagine_MapReduce/include
echo CPLUS_INCLUDE_PATH is $CPLUS_INCLUDE_PATH