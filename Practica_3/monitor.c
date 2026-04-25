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

/* ── Message queue ────────────────────────────────────────────────────────── */
#define MQ_NAME      "/miner_rush_queue"
#define MQ_MAX_MSG   10          /* Maximum messages in the queue            */
#define MAX_MINERS   10         /* Assumed upper bound on concurrent miners */

/**
 * @struct MQBlock
 * @brief Block received from the winning miner via message queue.
 * Must match the definition in miner.c exactly.
 */
typedef struct
{
    int target;   /* Target of the round                              */
    int solution; /* Solution found by the winner                     */
    int is_final; /* 1 → termination sentinel, 0 → normal round data */
} MQBlock;

/* ── Globals ──────────────────────────────────────────────────────────────── */
static mqd_t mq = (mqd_t)-1;          /* Message queue (created by Comprobador) */

/* Signal flag used by the Monitor child to detect shutdown */
static volatile sig_atomic_t fin_sistema = 0;

/* ── Signal handler ───────────────────────────────────────────────────────── */
/**
 * @brief Handles SIGUSR1 in the Monitor child to signal end of system.
 */
static void handler_fin(int sig)
{
    if (sig == SIGUSR1)
        fin_sistema = 1;
}

/* ── Resource management ──────────────────────────────────────────────────── */

/**
 * @brief Creates the POSIX message queue that the miners will use to send
 * blocks to the Comprobador.  Called once, before fork().
 * If the queue already exists (from a previous crashed run) it is removed
 * and recreated so the attributes are always correct.
 */
static void creacionRecursos(void)
{
    struct mq_attr attr;
    attr.mq_flags   = 0;
    attr.mq_maxmsg  = MQ_MAX_MSG;
    attr.mq_msgsize = sizeof(MQBlock);
    attr.mq_curmsgs = 0;

    /* Remove any stale queue from a previous run */
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
}

/* ── Main ─────────────────────────────────────────────────────────────────── */

int main(int argc, char *argv[])
{
    int  comprobacion_lag;
    int  monitor_lag;
    pid_t pid;

    if (argc != 3)
    {
        fprintf(stderr, "./%s <LAG_COMPROBADOR> <LAG_MONITOR>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    comprobacion_lag = atoi(argv[1]);
    monitor_lag      = atoi(argv[2]);

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
        struct sigaction act;
        act.sa_handler = handler_fin;
        act.sa_flags   = 0;
        sigemptyset(&act.sa_mask);
        sigaction(SIGUSR1, &act, NULL);

        /* The Monitor child does not use the queue directly */
        mq_close(mq);

        printf("[%d] Printing blocks ...\n", getpid());

        /*
         * Apartado b): simple reception loop.
         * In the final version (apartado c) this loop will extract blocks
         * from shared memory using the producer-consumer scheme.
         */
        while (!fin_sistema)
        {
            usleep(monitor_lag * 1000);
            /* Placeholder: in part c), consume one block from shared memory
             * and print it here with the required format:
             *   "Solution accepted: %08d --> %08d"
             *   "Solution rejected: %08d !-> %08d"
             */
        }

        printf("[%d] Finishing\n", getpid());
        exit(EXIT_SUCCESS);
    }

    /* ── COMPROBADOR (parent) ───────────────────────────────────────────── */
    else
    {
        MQBlock block;
        int     is_valid;

        while (1)
        {
            /* Blocking receive: wait until a miner sends a block */
            if (mq_receive(mq, (char *)&block, sizeof(MQBlock), NULL) == -1)
            {
                if (errno == EINTR)
                    continue;   /* Interrupted by a signal — retry */
                perror("mq_receive");
                break;
            }

            /* Termination sentinel: the last miner is done */
            if (block.is_final)
            {
                /*
                 * In part c), insert a termination sentinel into the
                 * shared-memory buffer here so Monitor also exits cleanly.
                 * For part b), simply signal the Monitor child with SIGUSR1.
                 */
                kill(pid, SIGUSR1);
                break;
            }

            /* Validate the solution independently (do not trust the miner) */
            is_valid = (pow_hash(block.solution) == block.target);

            /*
             * Apartado b): print the received block directly.
             * In part c), this will be replaced by inserting the block into
             * shared memory (producer half of producer-consumer).
             */
            if (is_valid)
                printf("Solution accepted: %08d --> %08d\n",
                       block.target, block.solution);
            else
                printf("Solution rejected: %08d !-> %08d\n",
                       block.target, block.solution);

            /* Wait the configured lag between checks */
            usleep(comprobacion_lag * 1000);
        }

        /* Wait for the Monitor child to finish */
        wait(NULL);

        /* Clean up the queue (miners do NOT do this) */
        limpiezaRecursos();
        exit(EXIT_SUCCESS);
    }
}