TARGET := main.bin
INCLUDES := ../lib
LIBS := threadpool
LPATH := ../lib

%.bin: %.c
	$(CC) $< -O0 -I$(INCLUDES) -l$(LIBS) -L$(INCLUDES) -o $@

clean:
	rm $(TARGET)

all: $(TARGET)

run: all
	LD_LIBRARY_PATH=$(abspath ../lib):$(LD_LIBRARY_PATH) ./$(TARGET)