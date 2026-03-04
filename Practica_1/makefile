CC = gcc
CFLAGS = -Wall -Wextra -pedantic -g -pthread 
TARGET = miner
all: $(TARGET)

$(TARGET): miner.o pow.o
	$(CC) $(CFLAGS) -o $(TARGET) miner.o pow.o

miner.o: miner.c pow.h
	$(CC) $(CFLAGS) -c miner.c

pow.o: pow.c pow.h
	$(CC) $(CFLAGS) -c pow.c

clean:
	rm -f *.o $(TARGET) *.log
