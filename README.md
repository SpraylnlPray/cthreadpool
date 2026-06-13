Basic Threadpool in C

## How to build
### Build the shared object
```
cd lib
make all
```
## Build the binary
```
cd bin
make all
```
## How to run
```
LD_LIBRARY_PATH=$(pwd)/lib:$LD_LIBRARY_PATH ./bin/main.bin
ctrl + c ends the program (after all workers completed)
kill -SIGUSR1 $(pgrep main.bin) adds a workload to the queue (a simple function that loops for some time)
```