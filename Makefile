.PHONY: build

system_file_name=./thirdparty/Imagine_System
tool_file_name=./thirdparty/Imagine_Tool

all: clean init prepare build

init:
	python3 init.py
prepare:
ifneq (${wildcard ${system_file_name}},)
	@echo "Imagine_System exists"
else
	@echo -e "\033[;31mImagine_System NOT exist, Please exucete make init to init it\033[0m"
	exit 1
endif
ifneq (${wildcard ${tool_file_name}},)
	@echo "Imagine_Tool exists"
else
	@echo -e "\033[;31mImagine_Tool NOT exist, Please exucete make init to init it\033[0m"
	exit 1
endif
	cd ${system_file_name} && make prepare
	cd ${tool_file_name} && make prepare

proto_init:
	./proto_generate.sh

build:
	cd build && cmake -DBUILD_MAPREDUCE=OFF .. && make imagine_mapreduce

mapreduce:
	cd build && cmake -DBUILD_MAPREDUCE=ON .. && make imagine_mapreduce

generator:
	cd build && cmake -DBUILD_RPC_SERVICE_GENERATOR=ON .. && make imagine_rpc_service_generator

clean:
	cd build && make clean
