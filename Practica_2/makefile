CC = gcc
CFLAGS = -Wall -g -pthread
LIBS = -lpthread

TARGET = miner

SRCS = miner.c pow.c
OBJS = $(SRCS:.c=.o)

all: $(TARGET) 

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LIBS)

%.o: %.c pow.h
	$(CC) $(CFLAGS) -c $< -o $@

valgrind: $(TARGET)
	valgrind --leak-check=full ./miner 5 4 & ./miner 5 4


test: $(TARGET)
	$ ./miner 6 7 & ./miner 3 13 & ./miner 5 2 & ./miner 8 10

clean:
	rm -f $(TARGET) $(OBJS) *.txt target.tgt voting.vot *.log

.PHONY: all clean test