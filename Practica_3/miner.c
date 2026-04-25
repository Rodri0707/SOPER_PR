/**
 * @file miner.c
 * @brief Multi-threaded miner with a forked Logger child.
 *
 * Each miner process:
 *   1. Registers itself in MINERS_LOG (protected by a named semaphore).
 *   2. Forks a Logger child that writes round results to <PID>.txt via a pipe.
 *   3. Participates in successive POW rounds until SIGALRM fires.
 *   4. The round winner sends the result to the Comprobador via a message queue.
 *   5. The last miner to exit sends the termination sentinel and cleans up
 *      semaphores (the Comprobador unlinks the queue).
 *
 * @author Rodrigo Díaz, Daniel Martinez
 * @date March 2026
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <signal.h>
#include <mqueue.h>
#include "pow.h"
#include "structs.h"
#include <sys/mman.h>

/* ── Signal flags ───────────────────────────────────────────────────────── */

static volatile sig_atomic_t got_sigusr1 = 0;
static volatile sig_atomic_t got_sigusr2 = 0;
static volatile sig_atomic_t time_is_up = 0;

/* ══════════════════════════════════════════════════════════════════════════
 * SIGNAL HANDLING
 * ══════════════════════════════════════════════════════════════════════════ */

static void signal_handler(int sig)
{
    if (sig == SIGUSR1)
        got_sigusr1 = 1;
    if (sig == SIGUSR2)
        got_sigusr2 = 1;
    if (sig == SIGALRM)
        time_is_up = 1;
}

static void setup_signals(void)
{
    struct sigaction act;
    act.sa_handler = signal_handler;
    act.sa_flags = SA_RESTART;
    sigemptyset(&act.sa_mask);

    if (sigaction(SIGUSR1, &act, NULL) == -1 ||
        sigaction(SIGUSR2, &act, NULL) == -1 ||
        sigaction(SIGALRM, &act, NULL) == -1)
    {
        perror("sigaction");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }
}

/* ══════════════════════════════════════════════════════════════════════════
 * SHARED MEMORY
 * ══════════════════════════════════════════════════════════════════════════ */

static MemoriaCompartida *open_shm(void)
{
    int fd_shm = shm_open(SHM_NAME, O_RDWR, 0); // Solo abrimos, no creamos
    if (fd_shm == -1)
    {
        fprintf(stderr, "Error: El Monitor no está ejecutándose.\n");
        exit(EXIT_FAILURE);
    }

    MemoriaCompartida *shm_ptr = mmap(NULL, sizeof(MemoriaCompartida),
                                      PROT_READ | PROT_WRITE, MAP_SHARED, fd_shm, 0);
    close(fd_shm); // Se cierra el descriptor, el puntero se queda

    if (shm_ptr == MAP_FAILED)
    {
        perror("mmap minero");
        exit(EXIT_FAILURE);
    }

    return shm_ptr;
}

/* ══════════════════════════════════════════════════════════════════════════
 * MESSAGE QUEUE
 * ══════════════════════════════════════════════════════════════════════════ */

static mqd_t open_mq()
{
    mqd_t mq = mq_open(MQ_NAME, O_WRONLY);
    if (mq == (mqd_t)-1)
    {
        fprintf(stderr, "Error: Monitor is not running. Please start monitor first.\n");
        exit(EXIT_FAILURE);
    }
    return mq;
}

static void send_block(mqd_t mq, int target, int solution)
{
    MQBlock block;
    block.target = target;
    block.solution = solution;
    block.is_final = 0;
    if (mq_send(mq, (char *)&block, sizeof(MQBlock), 1) == -1)
        perror("mq_send");
}

static void send_termination_block(mqd_t mq)
{
    MQBlock block;
    memset(&block, 0, sizeof(MQBlock));
    block.is_final = 1;
    if (mq_send(mq, (char *)&block, sizeof(MQBlock), 0) == -1)
        perror("mq_send (termination)");
}

/* ══════════════════════════════════════════════════════════════════════════
 * THREAD MANAGEMENT
 * ══════════════════════════════════════════════════════════════════════════ */

static void *pow_func(void *arg)
{
    ThreadArgs *a = (ThreadArgs *)arg;
    int i;

    for (i = a->start; i < a->end; i++)
    {
        if (got_sigusr2 || time_is_up)
            pthread_exit(NULL);
        if (*(a->solution) != NO_SOLUTION)
            pthread_exit(NULL);
        if (pow_hash(i) == a->target)
        {
            *(a->solution) = i;
            pthread_exit(NULL);
        }
    }
    return NULL;
}

static void alloc_threads(int n_threads, int *solution,
                          pthread_t **threads, ThreadArgs **args)
{
    int i, range;

    *threads = calloc(n_threads, sizeof(pthread_t));
    *args = calloc(n_threads, sizeof(ThreadArgs));
    if (!*threads || !*args)
    {
        perror("calloc");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    range = POW_LIMIT / n_threads;
    for (i = 0; i < n_threads; i++)
    {
        (*args)[i].start = i * range;
        (*args)[i].end = (i + 1) * range;
        (*args)[i].solution = solution;
    }
    (*args)[n_threads - 1].end = POW_LIMIT;
}

static void run_threads(pthread_t *threads, ThreadArgs *args,
                        int n_threads, int target)
{
    int j;
    for (j = 0; j < n_threads; j++)
    {
        args[j].target = target;
        pthread_create(&threads[j], NULL, pow_func, &args[j]);
    }
    for (j = 0; j < n_threads; j++)
        pthread_join(threads[j], NULL);
}

/* ══════════════════════════════════════════════════════════════════════════
 * MINER REGISTRY
 * ══════════════════════════════════════════════════════════════════════════ */

static int register_miner(pid_t pid, MemoriaCompartida *shm_ptr)
{
    int is_first = 0;
    int i;

    sem_wait(&shm_ptr->sem_miners);

    if (shm_ptr->num_mineros_activos == 0)
    {
        is_first = 1;
    }

    for (i = 0; i < MAX_MINERS; i++)
    {
        if (shm_ptr->pids_activos[i] == 0)
        {
            shm_ptr->pids_activos[i] = pid;
            shm_ptr->pids_carteras[i] = pid;
            shm_ptr->monedas[i] = 0;
            shm_ptr->num_mineros_activos++;
            break;
        }
    }

    if (i == MAX_MINERS)
    {
        fprintf(stderr, "Error: Límite máximo de mineros (%d) alcanzado.\n", MAX_MINERS);
        sem_post(&shm_ptr->sem_miners);
        return -1;
    }

    /* Imprimimos el estado por pantalla (sustituye a leer el fichero) */
    printf("Miner %d added to system\n", (int)pid);
    for (i = 0; i < MAX_MINERS; i++)
    {
        if (shm_ptr->pids_activos[i] != 0)
        {
            printf(" Miner PID: %d\n", shm_ptr->pids_activos[i]);
        }
    }

    /* Liberamos el semáforo */
    sem_post(&shm_ptr->sem_miners);

    return is_first;
}

static int unregister_miner(pid_t pid, MemoriaCompartida *shm_ptr, mqd_t mq)
{
    int remaining = 0;
    int i;

    sem_wait(&shm_ptr->sem_miners);

    for (i = 0; i < MAX_MINERS; i++)
    {
        if (shm_ptr->pids_activos[i] == pid)
        {
            shm_ptr->pids_activos[i] = 0;
            shm_ptr->num_mineros_activos--;
            break;
        }
    }

    remaining = shm_ptr->num_mineros_activos;

    printf("Miner %d exited system\n", (int)pid);

    /* Si aún quedan mineros, imprimimos la lista actualizada (igual que haciamos con el fichero) */
    if (remaining > 0)
    {
        for (i = 0; i < MAX_MINERS; i++)
        {
            if (shm_ptr->pids_activos[i] != 0)
            {
                printf(" Miner PID: %d\n", shm_ptr->pids_activos[i]);
            }
        }
    }

    sem_post(&shm_ptr->sem_miners);
    
    if (remaining == 0)
        send_termination_block(mq);

    /* Devolvemos el número de mineros restantes, para saber si enviamos el bloque de finalización */
    return remaining;
}

static int count_miners(MemoriaCompartida *shm_ptr)
{
    int total = 0;

    sem_wait(&shm_ptr->sem_miners);
    total = shm_ptr->num_mineros_activos;
    sem_post(&shm_ptr->sem_miners);

    return total;
}

/* ══════════════════════════════════════════════════════════════════════════
 * SIGNAL BROADCASTING
 * ══════════════════════════════════════════════════════════════════════════ */

/**
 * @brief Sends sig to every PID in MINERS_LOG, skipping exclude_pid (0 = none).
 */
static void broadcast_signal(MemoriaCompartida *shm_ptr, int sig, pid_t exclude_pid)
{
    int i;

    sem_wait(&shm_ptr->sem_miners);

    for (i = 0; i < MAX_MINERS; i++)
    {
        if (shm_ptr->pids_activos[i] != 0 && shm_ptr->pids_activos[i] != exclude_pid)
        {
            kill(shm_ptr->pids_activos[i], sig); // ¡Le disparamos!
        }
    }

    sem_post(&shm_ptr->sem_miners);
}

/* ══════════════════════════════════════════════════════════════════════════
 * TARGET FILE HELPERS
 * ══════════════════════════════════════════════════════════════════════════ */

static int read_target(MemoriaCompartida *shm_ptr)
{
    int val = 0;

    sem_wait(&shm_ptr->sem_target);
    val = shm_ptr->objetivo_actual;
    sem_post(&shm_ptr->sem_target);
    return val;
}

static void write_target(MemoriaCompartida *shm_ptr, int val)
{

    sem_wait(&shm_ptr->sem_target);
    shm_ptr->objetivo_actual = val;
    sem_post(&shm_ptr->sem_target);
}

static int read_solution(MemoriaCompartida *shm_ptr)
{
    int val = 0;
    sem_wait(&shm_ptr->sem_target);
    val = shm_ptr->solucion_propuesta;
    sem_post(&shm_ptr->sem_target);
    return val;
}

static void write_solution(MemoriaCompartida *shm_ptr, int val)
{
    sem_wait(&shm_ptr->sem_target);
    shm_ptr->solucion_propuesta = val;
    sem_post(&shm_ptr->sem_target);
}

/* ══════════════════════════════════════════════════════════════════════════
 * VOTING
 * ══════════════════════════════════════════════════════════════════════════ */

static void cast_vote(int target, int proposed, MemoriaCompartida *shm_ptr)
{

    char v = (pow_hash(proposed) == target && rand() % 100 >= 20) ? 'Y' : 'N';

    sem_wait(&shm_ptr->sem_votes);

    if (shm_ptr->num_votos_recibidos < MAX_MINERS)
    {
        shm_ptr->votos[shm_ptr->num_votos_recibidos] = v;
        shm_ptr->num_votos_recibidos++;
    }
    sem_post(&shm_ptr->sem_votes);
}

static void collect_votes(MemoriaCompartida *shm_ptr, int *yes_out, int *total_out, char *buf)
{
    int expected, current, yes, retries = 0, i, pos;
    const int MAX_RETRIES = 50;
    struct timespec wait = {0, 100L};

    /* Votos esperados: todos menos ganador*/
    expected = count_miners(shm_ptr) - 1;

    current = 0;
    yes = 0;
    buf[0] = '\0';

    /* Espera activa a que llegue los botos o nos cansemos*/
    while (current < expected && retries < MAX_RETRIES)
    {
        nanosleep(&wait, NULL);
        retries++;

        yes = 0;
        pos = 0;

        sem_wait(&shm_ptr->sem_votes);

        current = shm_ptr->num_votos_recibidos;

        for (i = 0; i < current; i++)
        {
            if (shm_ptr->votos[i] == 'Y')
            {
                yes++;
            }

            /* Construimos el string añadiendo la letra y un espacio */
            buf[pos++] = shm_ptr->votos[i];
            buf[pos++] = ' ';
        }

        buf[pos] = '\0';
        sem_post(&shm_ptr->sem_votes);
    }

    *yes_out = yes;
    *total_out = current;
}

/* ══════════════════════════════════════════════════════════════════════════
 * ROUND LOGIC: LOSER PATH
 * ══════════════════════════════════════════════════════════════════════════ */

/**
 * @brief Waits for SIGUSR2, then reads the winner's solution and casts a vote.
 * @return 0 on success, -1 if SIGALRM fired before SIGUSR2.
 */
static int loser_wait_and_vote(MemoriaCompartida *shm_ptr, int target)
{
    sigset_t wait_mask, block_sigusr2;
    int winner_sol;

    sigemptyset(&wait_mask);
    sigemptyset(&block_sigusr2);
    sigaddset(&block_sigusr2, SIGUSR2);

    /* Nos dormimos esperando a que el ganador nos envíe SIGUSR2 */
    sigprocmask(SIG_BLOCK, &block_sigusr2, NULL);
    while (!got_sigusr2 && !time_is_up)
        sigsuspend(&wait_mask);
    sigprocmask(SIG_UNBLOCK, &block_sigusr2, NULL);

    /* Si nos despierta el reloj y no hay ganador, abortamos */
    if (time_is_up && !got_sigusr2)
        return -1;

    got_sigusr2 = 0;

    /* Leemos la solución propuesta por el ganador */
    winner_sol = read_solution(shm_ptr);

    cast_vote(target, winner_sol, shm_ptr);

    return 0;
}

/* ══════════════════════════════════════════════════════════════════════════
 * ROUND LOGIC: WINNER PATH
 * ══════════════════════════════════════════════════════════════════════════ */

/**
 * @brief Full winner workflow: collect votes, validate, send MQ block,
 * write pipe log, release winner semaphore, start next round.
 */
static void winner_round(MemoriaCompartida *shm_ptr, int solution, int target,
                         int id_round, int *coins,
                         mqd_t mq, int fd_log, int fd_ack)
{
    int yes = 0, total = 0;
    char vote_str[256] = "";
    char ack_buf[MAX_SIZE];
    MessagePipeline msg;

    collect_votes(shm_ptr, &yes, &total, vote_str);

    if (total == 0)
    {
        /* Somos el único minero o nadie votó: restaurar objetivo y relanzar ronda */
        write_target(shm_ptr, target);
        sem_post(&shm_ptr->sem_winner);
        broadcast_signal(shm_ptr, SIGUSR1, 0);
        got_sigusr1 = 1;
        return;
    }

    if (yes >= (total / 2.0))
    {
        printf("Winner %d => [ %s] => Accepted\n", getpid(), vote_str);
        msg.is_valid = 1;
        (*coins)++;
        sem_wait(&shm_ptr->sem_miners);
        for (int i = 0; i < MAX_MINERS; i++)
        {
            if (shm_ptr->pids_carteras[i] == getpid())
            {
                shm_ptr->monedas[i] = *coins;
                break;
            }
        }
        sem_post(&shm_ptr->sem_miners);
        write_target(shm_ptr, (int)pow_hash(solution));
    }
    else
    {
        printf("Winner %d => [ %s] => Rejected\n", getpid(), vote_str);
        msg.is_valid = 0;
        write_target(shm_ptr, target); /* Restauramos el objetivo original para reintentar */
    }

    /* Enviamos el bloque al Comprobador por la Cola de Mensajes (Apartado b) */
    send_block(mq, target, solution);

    /* Enviamos los datos a nuestro proceso Logger hijo por la tubería (Pipe) */
    msg.id_round = id_round;
    msg.target = target;
    msg.solucion = solution;
    msg.votes_yes = yes;
    msg.votes_total = total;
    msg.coins = *coins;

    if (write(fd_log, &msg, sizeof(MessagePipeline)) == -1)
        perror("write pipe");
    read(fd_ack, ack_buf, MAX_SIZE);

    sem_post(&shm_ptr->sem_winner);

    /* Lanzamos la señal SIGUSR1 a todos para empezar la siguiente ronda */
    broadcast_signal(shm_ptr, SIGUSR1, 0);
    got_sigusr1 = 1;
}

/* ══════════════════════════════════════════════════════════════════════════
 * LOGGER CHILD
 * ══════════════════════════════════════════════════════════════════════════ */

static void logger_child(int fd_read, int fd_ack, MemoriaCompartida *shm_ptr)
{
    char filename[50];
    int log_fd;
    MessagePipeline msg;
    int i;

    snprintf(filename, sizeof(filename), "%d.txt", getppid());
    log_fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (log_fd == -1)
    {
        perror("open log");
        exit(EXIT_FAILURE);
    }

    while (read(fd_read, &msg, sizeof(MessagePipeline)) > 0)
    {
        dprintf(log_fd, "Id: %d\n", msg.id_round);
        dprintf(log_fd, "Winner: %d\n", getppid());
        dprintf(log_fd, "Target: %08d\n", msg.target);
        dprintf(log_fd, "Solution: %08d (%s)\n",
                msg.solucion, msg.is_valid ? "validated" : "rejected");
        dprintf(log_fd, "Votes: %d/%d\n", msg.votes_yes, msg.votes_total);

        /* Leer todas las carteras de la memoria compartida */
        dprintf(log_fd, "Wallets: ");

        /* Bloqueamos el acceso para no leer mientras otro minero se registra o actualiza */
        sem_wait(&shm_ptr->sem_miners);

        for (i = 0; i < MAX_MINERS; i++)
        {
            if (shm_ptr->pids_carteras[i] != 0)
            {
                dprintf(log_fd, "%d:%d ", shm_ptr->pids_carteras[i], shm_ptr->monedas[i]);
            }
        }

        sem_post(&shm_ptr->sem_miners);
        dprintf(log_fd, "\n\n");

        write(fd_ack, "OK", 3);
    }

    close(log_fd);
    close(fd_read);
    close(fd_ack);
    exit(EXIT_SUCCESS);
}

/* ══════════════════════════════════════════════════════════════════════════
 * FIRST-MINER INITIALISATION
 * ══════════════════════════════════════════════════════════════════════════ */

static void first_miner_init(MemoriaCompartida *shm_ptr)
{
    struct timespec wait = {0, 100L};

    while (count_miners(shm_ptr) < 2 && !time_is_up)
        nanosleep(&wait, NULL);

    if (time_is_up)
        return;

    write_target(shm_ptr, 0);

    broadcast_signal(shm_ptr, SIGUSR1, 0);

    got_sigusr1 = 1;
}

/* ══════════════════════════════════════════════════════════════════════════
 * RESOURCE CLEANUP
 * ══════════════════════════════════════════════════════════════════════════ */

static void free_resources(MemoriaCompartida *shm_ptr, mqd_t mq,
                           pthread_t *threads, ThreadArgs *args)
{

    /* Cerramos nuestra conexión con la cola de mensajes */
    mq_close(mq);

    /* Nos desvinculamos de la memoria compartida */
    if (shm_ptr != MAP_FAILED)
    {
        munmap(shm_ptr, sizeof(MemoriaCompartida));
    }

    /* Liberamos la memoria dinámica local de nuestros hilos */
    free(threads);
    free(args);
}

/* ══════════════════════════════════════════════════════════════════════════
 * MAIN
 * ══════════════════════════════════════════════════════════════════════════ */

int main(int argc, char *argv[])
{
    int n_secs, n_threads;
    int found_solution, target, is_winner;
    int id_round = 0, my_coins = 0;
    int is_first;
    int fd1[2], fd2[2];

    pthread_t *threads = NULL;
    ThreadArgs *args = NULL;

    /* NUESTRA NUEVA ESTRUCTURA GLOBAL */
    MemoriaCompartida *shm_ptr = NULL;
    mqd_t mq;
    pid_t pid;

    sigset_t wait_mask, block_sigusr1;

    if (argc != 3)
    {
        fprintf(stderr, "./%s <N_SECS> <N_THREADS>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    n_secs = atoi(argv[1]);
    n_threads = atoi(argv[2]);

    if (n_secs <= 0 || n_threads <= 0)
    {
        printf("Input error\n");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    srand(time(NULL));

    /* ── Initialisation ─────────────────────────────────────────────────── */
    setup_signals();

    /* ABRIMOS LA MEMORIA Y LA COLA */
    shm_ptr = open_shm();
    mq = open_mq();

    alloc_threads(n_threads, &found_solution, &threads, &args);

    if (pipe(fd1) == -1 || pipe(fd2) == -1)
    {
        perror("pipe");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    /* 3. REGISTRO PASANDO EL PUNTERO */
    is_first = register_miner(getpid(), shm_ptr);

    /* Fork the Logger child */
    pid = fork();
    if (pid < 0)
    {
        perror("fork");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    if (pid == 0) /* ── LOGGER CHILD ────────────────────────────────────── */
    {
        close(fd1[1]);
        close(fd2[0]);
        mq_close(mq);
        free(threads);
        free(args);

        logger_child(fd1[0], fd2[1], shm_ptr);
    }

    /* ── MINER (parent) ─────────────────────────────────────────────────── */
    close(fd1[0]);
    close(fd2[1]);

    sigemptyset(&wait_mask);
    sigemptyset(&block_sigusr1);
    sigaddset(&block_sigusr1, SIGUSR1);

    alarm(n_secs);

    if (is_first)
        first_miner_init(shm_ptr);

    /* loop principal de minado */
    while (!time_is_up)
    {
        struct timespec idle = {0, 100L};

        /* Pausar si somos el unico minero */
        if (count_miners(shm_ptr) < 2)
        {
            nanosleep(&idle, NULL);
            continue;
        }

        /* Esperar la señal de inicio */
        sigprocmask(SIG_BLOCK, &block_sigusr1, NULL);
        while (!got_sigusr1 && !time_is_up)
            sigsuspend(&wait_mask);
        sigprocmask(SIG_UNBLOCK, &block_sigusr1, NULL);

        if (time_is_up)
            break;
        got_sigusr1 = 0;

        /* Setup */
        target = read_target(shm_ptr);
        id_round++;
        found_solution = NO_SOLUTION;
        is_winner = 0;

        /* Mining*/
        run_threads(threads, args, n_threads, target);

        if (time_is_up)
            break;

        if (found_solution != NO_SOLUTION && !got_sigusr2)
        {
            /* INTENTAR GANAR LA RONDA (Usando el semáforo de la shm) */
            if (sem_trywait(&shm_ptr->sem_winner) == 0)
            {
                is_winner = 1;

                /* LIMPIAR LOS VOTOS DE LA RONDA ANTERIOR */
                sem_wait(&shm_ptr->sem_votes);
                shm_ptr->num_votos_recibidos = 0;
                sem_post(&shm_ptr->sem_votes);

                /* Publicaar solution para que voten */
                write_solution(shm_ptr, found_solution);

                /* Parar miners */
                broadcast_signal(shm_ptr, SIGUSR2, getpid()); // 10. AVISAR A TODOS
            }
        }

        /* Winner / loser paths */
        if (!is_winner)
        {
            if (loser_wait_and_vote(shm_ptr, target) == -1) // 11. RUTINA PERDEDOR
                break;
        }
        else
        {
            winner_round(shm_ptr, found_solution, target,
                         id_round, &my_coins, mq, fd1[1], fd2[0]);
        }

        got_sigusr2 = 0;
    }

    close(fd1[1]);
    close(fd2[0]);
    wait(NULL);

    /* DESREGISTRO Y LIBERACIÓN */
    unregister_miner(getpid(), shm_ptr, mq);
    free_resources(shm_ptr, mq, threads, args);

    return 0;
}