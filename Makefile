all:
	cd build && make
init:
	cd build && make init
prepare:
	cd build && cmake .. && make prepare
built:
	cd build && cmake .. && make build
clean:
	cd build && make clean
