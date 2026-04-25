#ifndef MINER_H
#define MINER_H

#define _POSIX_C_SOURCE 200809L

#include <sys/types.h>
#include <semaphore.h>

/* --- MACROS Y CONSTANTES --- */
#define NO_SOLUTION -1
#define MAX_SIZE 20

/* POSIX named semaphores */
#define SEM_NAME "/miners_mutex"
#define SEM_TARGET "/target_mutex"
#define SEM_WINNER "/winner_mutex"
#define SEM_VOTES "/vote_mutex"

/* Archivos del sistema (pronto desaparecerán en el minero gracias a la shm) */
#define MINERS_LOG "miners.log"
#define TARGET_FILE "target.tgt"
#define VOTING_FILE "voting.vot"

/* Recursos Compartidos POSIX */
#define MQ_NAME "/miner_rush_queue"
#define MQ_MAX_MSG 7 
#define SHM_NAME "/miner_rush_shm"

/* Tamaños máximos */
#define MAX_MINERS 10
#define CAPACIDAD_BUFFER 6

/* --- ESTRUCTURAS DE DATOS (Minero -> Comprobador) --- */

/**
 * @struct Semaphores
 * @brief Groups all four named semaphores used by the system so they can be
 * passed as a single argument instead of four separate pointers.
 */
typedef struct
{
    sem_t *miners;  /* Protects MINERS_LOG and the PID list        */
    sem_t *target;  /* Protects TARGET_FILE                         */
    sem_t *winner;  /* Binary mutex: only one winner per round      */
    sem_t *votes;   /* Protects VOTING_FILE                         */
} Semaphores;
 

typedef struct {
    int target;
    int start;
    int end;
    int *solution;
} ThreadArgs;

typedef struct {
    int id_round;
    int target;
    int solucion;
    int is_valid;
    int votes_yes;
    int votes_total;
    int coins;
} MessagePipeline;

typedef struct {
    int target;   
    int solution; 
    int is_final; 
} MQBlock;

/* --- ESTRUCTURAS DE DATOS (Memoria Compartida) --- */

// 1. Movemos esto arriba para que se pueda usar dentro del buffer
typedef struct {
    int objetivo;
    int solucion;
    int es_valido;
    int es_finalizacion;
} BloqueProdCons;

// 2. Unificamos todo en una sola estructura gigante para hacer un único mmap
typedef struct {
    
    /* === PARTE 1: INFORMACIÓN GLOBAL DEL SISTEMA (MinerCompStruct) === */
    int objetivo_actual;
    int solucion_propuesta;
    int validacion_correcta;
    
    int pids_activos[MAX_MINERS];
    int num_mineros_activos;
    
    int pids_carteras[MAX_MINERS];
    int monedas[MAX_MINERS];
    
    char votos[MAX_MINERS];
    int num_votos_recibidos;

    /* === PARTE 2: BUFFER CIRCULAR (CompMonStruct) === */
    BloqueProdCons buffer[CAPACIDAD_BUFFER];
    int in;   
    int out;  
    
    /* === PARTE 3: SEMÁFOROS SIN NOMBRE === */
    sem_t sem_mutex_buffer; 
    sem_t sem_empty;
    sem_t sem_fill;
    
    sem_t sem_mutex_global; 

} MemoriaCompartida;

#endif /* MINER_H */