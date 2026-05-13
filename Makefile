TARGET := main.bin

%.bin: %.c
	$(CC) $< -g -O0 -o $@

clean:
	rm $(TARGET)

all: $(TARGET)