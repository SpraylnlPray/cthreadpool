TARGET := threadpool.so

%.o: %.c
	$(CC) -c -Wall -Werror -fpic $<

%.so: %.o
	$(CC) -shared -o $(addprefix lib,$@) $<

all: $(TARGET)

clean:
	rm $(addprefix lib,$(TARGET))