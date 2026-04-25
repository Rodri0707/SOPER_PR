CC = gcc
CFLAGS = -Wall -g -pthread
LIBS = -lpthread -lrt

TARGETS = miner monitor

all: $(TARGETS) 

miner: miner.o pow.o
	$(CC) $(CFLAGS) -o miner miner.o pow.o $(LIBS)

monitor: monitor.o pow.o
	$(CC) $(CFLAGS) -o monitor monitor.o pow.o $(LIBS)

%.o: %.c pow.h
	$(CC) $(CFLAGS) -c $< -o $@

test: miner monitor
	./monitor 500 300 &
	sleep 0.3
	./miner 6 4 & ./miner 6 4 & ./miner 6 4

clean:
	rm -f $(TARGETS) *.o *.txt target.tgt voting.vot *.log
	rm -f /dev/mqueue/miner_rush_queue

.PHONY: all clean test valgrind