TARGET := main.bin

%.bin: %.c
	$(CC) $< -o $@

all: $(TARGET)