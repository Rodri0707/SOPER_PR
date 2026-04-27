CC = gcc
CFLAGS = -Wall -g -pthread
LIBS = -lpthread -lrt

# Archivos de cabecera (Header files)
DEPS = pow.h structs.h

TARGETS = miner monitor

all: $(TARGETS) 

# Regla para el ejecutable miner
miner: miner.o pow.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

# Regla para el ejecutable monitor
monitor: monitor.o pow.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

# Regla genérica para objetos, ahora depende también de structs.h
%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c $< -o $@

# Test automático mejorado
test: all
	@echo "Lanzando Monitor..."
	./monitor 10 5 & \
	sleep 1; \
	echo "Lanzando Mineros..."; \
	./miner 5 3 & ./miner 5 7 & ./miner 5 3 & ./miner 5 7 & ./miner 5 3 & ./miner 5 7; \
	wait
	@echo "Prueba finalizada."

# Limpieza profunda
clean:
	rm -f $(TARGETS) *.o *.txt *.log
	@echo "Limpiando colas y memoria compartida de sistema..."
	rm -f /dev/mqueue/miner_rush_queue
	rm -f /dev/shm/miner_rush_shm
	@# También borramos los archivos antiguos por si acaso
	rm -f target.tgt voting.vot miners.log

# Regla para buscar fugas de memoria (solo en el monitor/comprobador)
valgrind: all
	valgrind --leak-check=full --show-leak-kinds=all ./monitor 500 300

.PHONY: all clean test valgrind
