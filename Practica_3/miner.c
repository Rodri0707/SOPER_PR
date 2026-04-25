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
 * SEMAPHORE MANAGEMENT
 * ══════════════════════════════════════════════════════════════════════════ */

static void open_semaphores(Semaphores *s)
{
    s->miners = sem_open(SEM_NAME, O_CREAT, 0644, 1);
    s->target = sem_open(SEM_TARGET, O_CREAT, 0644, 1);
    s->winner = sem_open(SEM_WINNER, O_CREAT, 0644, 1);
    s->votes = sem_open(SEM_VOTES, O_CREAT, 0644, 1);

    if (s->miners == SEM_FAILED || s->target == SEM_FAILED ||
        s->winner == SEM_FAILED || s->votes == SEM_FAILED)
    {
        perror("sem_open");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }
}

static void close_semaphores(Semaphores *s)
{
    sem_close(s->miners);
    sem_close(s->target);
    sem_close(s->winner);
    sem_close(s->votes);
}

static void unlink_semaphores(void)
{
    sem_unlink(SEM_NAME);
    sem_unlink(SEM_TARGET);
    sem_unlink(SEM_WINNER);
    sem_unlink(SEM_VOTES);
}
/* ══════════════════════════════════════════════════════════════════════════
 * SHARED MEMORY
 * ══════════════════════════════════════════════════════════════════════════ */

static MemoriaCompartida* open_shm(void)
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

static mqd_t open_mq(Semaphores *s)
{
    mqd_t mq = mq_open(MQ_NAME, O_WRONLY);
    if (mq == (mqd_t)-1)
    {
        fprintf(stderr, "Error: Monitor is not running. Please start monitor first.\n");
        printf("Miner exited unexpectedly\n");
        close_semaphores(s);
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

static int register_miner(pid_t pid, sem_t *sem)
{
    FILE *f;
    char line[64];
    int is_first = 0;

    sem_wait(sem);

    f = fopen(MINERS_LOG, "r");
    if (!f)
        is_first = 1;
    else
        fclose(f);

    f = fopen(MINERS_LOG, "a");
    if (!f)
    {
        perror("fopen miners.log");
        sem_post(sem);
        return -1;
    }
    fprintf(f, "Miner PID: %d\n", (int)pid);
    fclose(f);

    printf("Miner %d added to system\n", (int)pid);
    f = fopen(MINERS_LOG, "r");
    if (f)
    {
        while (fgets(line, sizeof(line), f))
            printf(" %s", line);
        fclose(f);
    }

    sem_post(sem);
    return is_first;
}

static int unregister_miner(pid_t pid, sem_t *sem)
{
    FILE *f, *tmp;
    char line[64], pid_line[64];
    int remaining = 0;

    snprintf(pid_line, sizeof(pid_line), "Miner PID: %d\n", (int)pid);

    sem_wait(sem);
    f = fopen(MINERS_LOG, "r");
    if (!f)
    {
        sem_post(sem);
        return 0;
    }

    tmp = fopen("miners_tmp.log", "w");
    if (!tmp)
    {
        fclose(f);
        sem_post(sem);
        return 0;
    }

    while (fgets(line, sizeof(line), f))
        if (strcmp(line, pid_line) != 0)
        {
            fputs(line, tmp);
            remaining++;
        }

    fclose(f);
    fclose(tmp);

    if (remaining == 0)
    {
        remove("miners_tmp.log");
        remove(MINERS_LOG);
    }
    else
    {
        rename("miners_tmp.log", MINERS_LOG);
        f = fopen(MINERS_LOG, "r");
        if (f)
        {
            while (fgets(line, sizeof(line), f))
                printf(" %s", line);
            fclose(f);
        }
    }

    printf("Miner %d exited system\n", (int)pid);
    sem_post(sem);
    return remaining;
}

static int count_miners(sem_t *sem)
{
    FILE *f;
    char line[64];
    int total = 0;

    sem_wait(sem);
    f = fopen(MINERS_LOG, "r");
    if (f)
    {
        while (fgets(line, sizeof(line), f))
            total++;
        fclose(f);
    }
    sem_post(sem);
    return total;
}

/* ══════════════════════════════════════════════════════════════════════════
 * SIGNAL BROADCASTING
 * ══════════════════════════════════════════════════════════════════════════ */

/**
 * @brief Sends sig to every PID in MINERS_LOG, skipping exclude_pid (0 = none).
 */
static void broadcast_signal(sem_t *sem, int sig, pid_t exclude_pid)
{
    FILE *f;
    char line[64];
    int pid;

    sem_wait(sem);
    f = fopen(MINERS_LOG, "r");
    if (f)
    {
        while (fgets(line, sizeof(line), f))
            if (sscanf(line, "Miner PID: %d", &pid) == 1)
                if (pid != (int)exclude_pid)
                    kill((pid_t)pid, sig);
        fclose(f);
    }
    sem_post(sem);
}

/* ══════════════════════════════════════════════════════════════════════════
 * TARGET FILE HELPERS
 * ══════════════════════════════════════════════════════════════════════════ */

static int read_target(sem_t *sem_target)
{
    FILE *f;
    int val = 0;

    sem_wait(sem_target);
    f = fopen(TARGET_FILE, "r");
    if (f)
    {
        fscanf(f, "%d", &val);
        fclose(f);
    }
    sem_post(sem_target);
    return val;
}

static void write_target(sem_t *sem_target, int val)
{
    FILE *f;

    sem_wait(sem_target);
    f = fopen(TARGET_FILE, "w");
    if (f)
    {
        fprintf(f, "%d\n", val);
        fclose(f);
    }
    sem_post(sem_target);
}

/* ══════════════════════════════════════════════════════════════════════════
 * VOTING
 * ══════════════════════════════════════════════════════════════════════════ */

static void cast_vote(int target, int proposed, sem_t *sem_votes)
{
    FILE *f;
    char v = (pow_hash(proposed) == target && rand() % 100 >= 20) ? 'Y' : 'N';

    sem_wait(sem_votes);
    f = fopen(VOTING_FILE, "a");
    if (f)
    {
        fprintf(f, "%c ", v);
        fclose(f);
    }
    sem_post(sem_votes);
}

static void collect_votes(Semaphores *s,
                          int *yes_out, int *total_out, char *buf)
{
    FILE *f;
    char v;
    int expected, current, yes, len, retries = 0;
    const int MAX_RETRIES = 50;
    struct timespec wait = {0, 100L};

    expected = count_miners(s->miners) - 1;
    current = yes = 0;
    buf[0] = '\0';

    while (current < expected && retries < MAX_RETRIES)
    {
        nanosleep(&wait, NULL);
        retries++;
        current = yes = 0;
        buf[0] = '\0';

        sem_wait(s->votes);
        f = fopen(VOTING_FILE, "r");
        if (f)
        {
            while (fscanf(f, " %c", &v) == 1)
            {
                current++;
                if (v == 'Y')
                    yes++;
                len = strlen(buf);
                if (len < 250)
                {
                    buf[len] = v;
                    buf[len + 1] = ' ';
                    buf[len + 2] = '\0';
                }
            }
            fclose(f);
        }
        sem_post(s->votes);
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
static int loser_wait_and_vote(Semaphores *s, int target)
{
    sigset_t wait_mask, block_sigusr2;
    int winner_sol;

    sigemptyset(&wait_mask);
    sigemptyset(&block_sigusr2);
    sigaddset(&block_sigusr2, SIGUSR2);

    sigprocmask(SIG_BLOCK, &block_sigusr2, NULL);
    while (!got_sigusr2 && !time_is_up)
        sigsuspend(&wait_mask);
    sigprocmask(SIG_UNBLOCK, &block_sigusr2, NULL);

    if (time_is_up && !got_sigusr2)
        return -1;

    got_sigusr2 = 0;
    winner_sol = read_target(s->target);
    cast_vote(target, winner_sol, s->votes);
    return 0;
}

/* ══════════════════════════════════════════════════════════════════════════
 * ROUND LOGIC: WINNER PATH
 * ══════════════════════════════════════════════════════════════════════════ */

/**
 * @brief Full winner workflow: collect votes, validate, send MQ block,
 * write pipe log, release winner semaphore, start next round.
 */
static void winner_round(Semaphores *s, int solution, int target,
                         int id_round, int *coins,
                         mqd_t mq, int fd_log, int fd_ack)
{
    int yes = 0, total = 0;
    char vote_str[256] = "";
    char ack_buf[MAX_SIZE];
    MessagePipeline msg;

    collect_votes(s, &yes, &total, vote_str);

    if (total == 0)
    {
        sem_post(s->winner);
        return;
    }

    if (yes >= (total / 2.0))
    {
        printf("Winner %d => [ %s] => Accepted\n", getpid(), vote_str);
        msg.is_valid = 1;
        (*coins)++;
    }
    else
    {
        printf("Winner %d => [ %s] => Rejected\n", getpid(), vote_str);
        msg.is_valid = 0;
        write_target(s->target, target); /* restore target for retry */
    }

    send_block(mq, target, solution);

    msg.id_round = id_round;
    msg.target = target;
    msg.solucion = solution;
    msg.votes_yes = yes;
    msg.votes_total = total;
    msg.coins = *coins;
    if (write(fd_log, &msg, sizeof(MessagePipeline)) == -1)
        perror("write pipe");
    read(fd_ack, ack_buf, MAX_SIZE);

    sem_post(s->winner);

    /* Kick off the next round for everyone */
    broadcast_signal(s->miners, SIGUSR1, 0);
    got_sigusr1 = 1;
}

/* ══════════════════════════════════════════════════════════════════════════
 * LOGGER CHILD
 * ══════════════════════════════════════════════════════════════════════════ */

static void logger_child(int fd_read, int fd_ack)
{
    char filename[50];
    int log_fd;
    MessagePipeline msg;

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
        dprintf(log_fd, "Wallets: %d:%d\n\n", getppid(), msg.coins);
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

static void first_miner_init(Semaphores *s)
{
    struct timespec wait = {0, 100L};

    while (count_miners(s->miners) < 2 && !time_is_up)
        nanosleep(&wait, NULL);

    if (time_is_up)
        return;

    write_target(s->target, 0);
    broadcast_signal(s->miners, SIGUSR1, 0);
    got_sigusr1 = 1;
}

/* ══════════════════════════════════════════════════════════════════════════
 * RESOURCE CLEANUP
 * ══════════════════════════════════════════════════════════════════════════ */

static void free_resources(Semaphores *s, mqd_t mq,
                           int remaining, pthread_t *threads, ThreadArgs *args)
{
    close_semaphores(s);
    if (remaining == 0)
    {
        unlink_semaphores();
        send_termination_block(mq);
    }
    mq_close(mq);
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
    int remaining_miners, is_first;
    int fd1[2], fd2[2];

    pthread_t *threads = NULL;
    ThreadArgs *args = NULL;
    Semaphores sems;
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
    open_semaphores(&sems);
    mq = open_mq(&sems);
    alloc_threads(n_threads, &found_solution, &threads, &args);

    if (pipe(fd1) == -1 || pipe(fd2) == -1)
    {
        perror("pipe");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    is_first = register_miner(getpid(), sems.miners);

    /* ── Fork the Logger child ──────────────────────────────────────────── */
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
        close_semaphores(&sems);
        mq_close(mq);
        free(threads);
        free(args);
        logger_child(fd1[0], fd2[1]);
        /* never reached */
    }

    /* ── MINER (parent) ─────────────────────────────────────────────────── */
    close(fd1[0]);
    close(fd2[1]);

    sigemptyset(&wait_mask);
    sigemptyset(&block_sigusr1);
    sigaddset(&block_sigusr1, SIGUSR1);

    alarm(n_secs);

    if (is_first)
        first_miner_init(&sems);

    /* ── Main mining loop ───────────────────────────────────────────────── */
    while (!time_is_up)
    {
        struct timespec idle = {0, 100L};

        /* Pause if we are the only miner left */
        if (count_miners(sems.miners) < 2)
        {
            nanosleep(&idle, NULL);
            continue;
        }

        /* Wait for the start-of-round signal */
        sigprocmask(SIG_BLOCK, &block_sigusr1, NULL);
        while (!got_sigusr1 && !time_is_up)
            sigsuspend(&wait_mask);
        sigprocmask(SIG_UNBLOCK, &block_sigusr1, NULL);

        if (time_is_up)
            break;
        got_sigusr1 = 0;

        /* ── Round setup ────────────────────────────────────────────────── */
        target = read_target(sems.target);
        id_round++;
        found_solution = NO_SOLUTION;
        is_winner = 0;

        /* ── Mining ─────────────────────────────────────────────────────── */
        run_threads(threads, args, n_threads, target);

        if (time_is_up)
            break;

        /* ── Try to claim the winner slot (only one per round) ─────────── */
        if (found_solution != NO_SOLUTION && !got_sigusr2)
        {
            if (sem_trywait(sems.winner) == 0)
            {
                is_winner = 1;

                /* Clear voting file for this round */
                sem_wait(sems.votes);
                FILE *f = fopen(VOTING_FILE, "w");
                if (f)
                    fclose(f);
                sem_post(sems.votes);

                /* Publish solution so losers can vote */
                write_target(sems.target, found_solution);

                /* Stop other miners */
                broadcast_signal(sems.miners, SIGUSR2, getpid());
            }
        }

        /* ── Winner / loser paths ───────────────────────────────────────── */
        if (!is_winner)
        {
            if (loser_wait_and_vote(&sems, target) == -1)
                break;
        }
        else
        {
            winner_round(&sems, found_solution, target,
                         id_round, &my_coins, mq, fd1[1], fd2[0]);
        }

        got_sigusr2 = 0;
    }

    /* ── Cleanup ────────────────────────────────────────────────────────── */
    close(fd1[1]);
    close(fd2[0]);
    wait(NULL);

    remaining_miners = unregister_miner(getpid(), sems.miners);
    free_resources(&sems, mq, remaining_miners, threads, args);

    return 0;
}