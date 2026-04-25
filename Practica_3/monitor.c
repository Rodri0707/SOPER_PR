/**
 * @file monitor.c
 * @brief Monitor system for the Miner-Rush blockchain network.
 * Spawns two processes: Comprobador (parent) and Monitor (child).
 * - Comprobador: receives solution blocks from miners via message queue,
 *   validates them, and (in part c) passes them to Monitor via shared memory.
 * - Monitor: displays the unified output of the network.
 *
 * @details Apartado b): Comprobador creates the message queue with capacity 7,
 * receives MQBlock messages, validates them independently using pow_hash, and
 * prints the result. Monitor runs a simple loop. When a termination block
 * arrives, Comprobador signals Monitor (SIGUSR1) and cleans up all resources.
 *
 * @author Rodrigo Díaz, Daniel Martinez
 * @date March 2026
 */

#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>   
#include <mqueue.h>
#include <signal.h>
#include "pow.h"
#include "structs.h"

/* ── Globals ──────────────────────────────────────────────────────────────── */
static mqd_t mq = (mqd_t)-1; /* Message queue (created by Comprobador) */
MemoriaCompartida *shm_struct = MAP_FAILED;

/**
 * @brief Creates the POSIX message queue that the miners will use to send
 * blocks to the Comprobador.  Called once, before fork().
 * If the queue already exists (from a previous crashed run) it is removed
 * and recreated so the attributes are always correct.
 */
static void creacionRecursos(void)
{
    int fd_shm;
    struct mq_attr attr;
    attr.mq_flags = 0;
    attr.mq_maxmsg = MQ_MAX_MSG;
    attr.mq_msgsize = sizeof(MQBlock);
    attr.mq_curmsgs = 0;


    shm_unlink(SHM_NAME);
    /* Remove any stale queue from a previous run */
    if ((fd_shm = shm_open(SHM_NAME, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR)) == -1)
    {
        perror(" shm_open ");
        exit(EXIT_FAILURE);
    }

    if (ftruncate(fd_shm, sizeof(MemoriaCompartida)) == -1)
    {
        perror(" ftruncate ");
        shm_unlink(SHM_NAME);
        exit(EXIT_FAILURE);
    }

    shm_struct = mmap(NULL, sizeof(MemoriaCompartida), PROT_READ | PROT_WRITE, MAP_SHARED, fd_shm, 0);
    close(fd_shm);
    if (shm_struct == MAP_FAILED)
    {
        perror(" mmap ");
        shm_unlink(SHM_NAME);
        exit(EXIT_FAILURE);
    }
    shm_struct->in = 0;
    shm_struct->out = 0;
    shm_struct->num_mineros_activos = 0;
    shm_struct->objetivo_actual = 0; 

    sem_init(&shm_struct->sem_mutex_buffer, 1, 1);               
    sem_init(&shm_struct->sem_empty, 1, CAPACIDAD_BUFFER);       
    sem_init(&shm_struct->sem_fill, 1, 0);                       
    sem_init(&shm_struct->sem_miners, 1, 1);
    sem_init(&shm_struct->sem_target, 1, 1);
    sem_init(&shm_struct->sem_winner, 1, 1);
    sem_init(&shm_struct->sem_votes,  1, 1);            

    mq_unlink(MQ_NAME);

    mq = mq_open(MQ_NAME,
                 O_CREAT | O_RDONLY,
                 S_IRUSR | S_IWUSR,
                 &attr);
    if (mq == (mqd_t)-1)
    {
        perror("mq_open (monitor creation)");
        fprintf(stderr, "El monitor se cerró\n");
        exit(EXIT_FAILURE);
    }
}

/**
 * @brief Closes and unlinks the message queue.
 * Called only by the Comprobador (parent) after the Monitor child has exited.
 */
static void limpiezaRecursos(void)
{
    if (mq != (mqd_t)-1)
    {
        mq_close(mq);
        mq_unlink(MQ_NAME);
    }

    if (shm_struct != MAP_FAILED)
    {
        sem_destroy(&shm_struct->sem_mutex_buffer);
        sem_destroy(&shm_struct->sem_empty);
        sem_destroy(&shm_struct->sem_fill);
        sem_destroy(&shm_struct->sem_miners);
        sem_destroy(&shm_struct->sem_target);
        sem_destroy(&shm_struct->sem_winner);
        sem_destroy(&shm_struct->sem_votes);

        munmap(shm_struct, sizeof(MemoriaCompartida));
        shm_unlink(SHM_NAME);
    }
}

/* ── Main ─────────────────────────────────────────────────────────────────── */

int main(int argc, char *argv[])
{
    int comprobacion_lag;
    int monitor_lag;
    pid_t pid;

    if (argc != 3)
    {
        fprintf(stderr, "./%s <LAG_COMPROBADOR> <LAG_MONITOR>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    comprobacion_lag = atoi(argv[1]);
    monitor_lag = atoi(argv[2]);

    if (comprobacion_lag <= 0 || monitor_lag <= 0)
    {
        printf("Input error: Los retardos deben de ser mayores que 0\n");
        printf("El monitor se cerró\n");
        exit(EXIT_FAILURE);
    }

    /* Create the message queue before forking so both processes inherit the
     * descriptor (only Comprobador actually uses it). */
    creacionRecursos();

    pid = fork();
    if (pid < 0)
    {
        perror("Error en el Fork");
        printf("El monitor se cerró\n");
        limpiezaRecursos();
        exit(EXIT_FAILURE);
    }

    /* ── MONITOR (child) ────────────────────────────────────────────────── */
    else if (pid == 0)
    {
        BloqueProdCons bloque_leido;

        /* El monitor no usa la cola*/
        mq_close(mq);

        printf("[%d] Printing blocks ...\n", getpid());

        
        while (1)
        {
            /* ZONA CONSUMIDOR (Extraer del Buffer) */
            sem_wait(&shm_struct->sem_fill);         /* Esperamos a que haya algo que leer */
            sem_wait(&shm_struct->sem_mutex_buffer); /* Bloqueamos el buffer */

            bloque_leido = shm_struct->buffer[shm_struct->out];
            shm_struct->out = (shm_struct->out + 1) % CAPACIDAD_BUFFER;

            sem_post(&shm_struct->sem_mutex_buffer); /* Desbloqueamos el buffer */
            sem_post(&shm_struct->sem_empty);        /* Avisamos de que hemos dejado un hueco libre */

            if (bloque_leido.es_finalizacion)
                break;

            /* Imprimimos el resultado */
            if (bloque_leido.es_valido)
                printf("Solution accepted: %08d --> %08d\n",
                       bloque_leido.objetivo, bloque_leido.solucion);
            else
                printf("Solution rejected: %08d !-> %08d\n",
                       bloque_leido.objetivo, bloque_leido.solucion);

            usleep(monitor_lag * 1000);
        }

        printf("[%d] Finishing\n", getpid());
        exit(EXIT_SUCCESS);
    }

    /* ── COMPROBADOR (parent) ───────────────────────────────────────────── */
    else
    {
        MQBlock block;
        int is_valid;

        while (1)
        {
            if (mq_receive(mq, (char *)&block, sizeof(MQBlock), NULL) == -1)
            {
                if (errno == EINTR) continue;
                perror("mq_receive");
                break;
            }

            /* Validamos la solución */
            is_valid = (block.is_final) ? 0 : (pow_hash(block.solution) == block.target);

            /* Preparamos el bloque para la memoria compartida */
            BloqueProdCons nuevo_bloque;
            nuevo_bloque.objetivo = block.target;
            nuevo_bloque.solucion = block.solution;
            nuevo_bloque.es_valido = is_valid;
            nuevo_bloque.es_finalizacion = block.is_final;

            /* ZONA PRODUCTOR (Insertar en Buffer) */
            sem_wait(&shm_struct->sem_empty);        /* Esperamos a que haya un hueco libre */
            sem_wait(&shm_struct->sem_mutex_buffer); /* Bloqueamos el buffer */

            shm_struct->buffer[shm_struct->in] = nuevo_bloque;
            shm_struct->in = (shm_struct->in + 1) % CAPACIDAD_BUFFER;

            sem_post(&shm_struct->sem_mutex_buffer); /* Desbloqueamos el buffer */
            sem_post(&shm_struct->sem_fill);         /* Avisamos de que hay un hueco lleno nuevo */

            /* Si era el último, salimos del bucle */
            if (block.is_final)
                break;

            usleep(comprobacion_lag * 1000);
        }

        wait(NULL);

        limpiezaRecursos();
        exit(EXIT_SUCCESS);
    }
}