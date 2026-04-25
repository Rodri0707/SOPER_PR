/**
 * @file miner.c
 * @brief Single multi-threaded miner and multi-process logger system.
 * Solves Proof of Work (POW) rounds by brute force using
 * pthreads and communication via pipes and message queues.
 * @details Libraries and Modules:
 * - Standard C Libraries: stdio.h, stdlib.h, string.h, time.h
 * - POSIX Libraries: sys/types.h, sys/wait.h, unistd.h, fcntl.h, pthread.h, mqueue.h
 * - External Modules: "pow.h" (provides pow_hash function and POW_LIMIT).
 * @author Rodrigo Díaz, Daniel Martinez
 * @date March 2026
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <signal.h>
#include <mqueue.h>
#include "pow.h"

#define NO_SOLUTION -1
#define MAX_SIZE 20

/* POSIX named semaphores */
#define SEM_NAME "/miners_mutex"
#define SEM_TARGET "/target_mutex"
#define SEM_WINNER "/winner_mutex"
#define SEM_VOTES "/vote_mutex"

#define MINERS_LOG "miners.log"  /* File with system info */
#define TARGET_FILE "target.tgt" /* File with the target */
#define VOTING_FILE "voting.vot" /* File with the votes */

/* Message queue for sending results to the monitor/checker */
#define MQ_NAME "/miner_rush_queue"
#define MAX_MINERS 10 /* Maximum number of concurrent miners assumed */

/**
 * @struct ThreadArgs
 * @brief Stores the necessary arguments for each mining thread.
 * Contains the search range assigned to each thread to divide the work.
 */
typedef struct
{
    int target;    /* Target to search */
    int start;     /* Starting on: lower bound of the search */
    int end;       /* Going on until: upper bound of the search */
    int *solution; /* Store solution: shared pointer to store the solution if found */
} ThreadArgs;

/**
 * @struct MessagePipeline
 * @brief Data packet sent from the Miner to the Logger through the pipe.
 * Data structure to be sent atomically through the pipe.
 */
typedef struct
{
    int id_round;
    int target;
    int solucion;
    int is_valid;
    int votes_yes;
    int votes_total;
    int coins;
} MessagePipeline;

/**
 * @struct MQBlock
 * @brief Block sent from the winning Miner to the Comprobador via message queue.
 * Contains the round result to be validated and logged by the monitor system.
 * The is_final flag signals the Comprobador that the system is shutting down.
 */
typedef struct
{
    int target;    /* Target of the round */
    int solution;  /* Solution found by the winner */
    int is_final;  /* 1 if this is the termination sentinel block, 0 otherwise */
} MQBlock;

/* * Global flags for asynchronous signal handling.
 * Declared as 'volatile sig_atomic_t' to ensure atomic reads and writes,
 * and to prevent compiler caching optimizations, since they can change
 * at any time due to an OS interrupt.
 */
static volatile sig_atomic_t got_sigusr1 = 0; /* Indicates the start of a new round */
static volatile sig_atomic_t got_sigusr2 = 0; /* Indicates that someone has won the round */
static volatile sig_atomic_t time_is_up = 0;  /* Indicates that the lifetime alarm has expired */

/**
 * @brief Routine executed by each thread to search for the target.
 * @param arg Pointer to the ThreadArgs structure containing the search limits.
 * @return NULL upon completion.
 */
void *pow_func(void *arg)
{
    ThreadArgs *args = (ThreadArgs *)arg;
    int i;

    /* Brute-force search within the assigned range */
    for (i = args->start; i < args->end; i++)
    {
        /* If another miner won (SIGUSR2) or the lifetime is up (SIGALRM) we abort */
        if (got_sigusr2 == 1 || time_is_up == 1)
        {
            pthread_exit(NULL);
        }

        /* We check if another thread *from our own process* has already found the solution */
        if (*(args->solution) != NO_SOLUTION)
        {
            pthread_exit(NULL);
        }

        /* Calculate the hash and check if it matches the target */
        if (pow_hash(i) == args->target)
        {
            *(args->solution) = i;
            pthread_exit(NULL);
        }
    }
    return NULL;
}

/**
 * @brief Registers the PID of the miner process in MINERS_LOG.
 * @param pid PID to register.
 * @param sem Already-opened semaphore (sem_t *).
 * @return 1 if it is the first miner in the system, 0 otherwise.
 */
static int register_miner(pid_t pid, sem_t *sem)
{
    FILE *f;
    char line[64];
    int is_first = 0;

    sem_wait(sem);
    /* Check if the file exists. If not, we are the first miner */
    f = fopen(MINERS_LOG, "r");
    if (f == NULL)
        is_first = 1;
    else
        fclose(f);

    f = fopen(MINERS_LOG, "a");
    if (f == NULL)
    {
        perror("Error opening " MINERS_LOG);
        sem_post(sem);
        return -1;
    }
    fprintf(f, "Miner PID: %d\n", (int)pid);
    fclose(f);

    printf("Miner %d added to system\n", (int)pid);

    /* Display the current list of miners */
    f = fopen(MINERS_LOG, "r");
    if (f != NULL)
    {
        while (fgets(line, sizeof(line), f) != NULL)
            printf(" %s", line);
        fclose(f);
    }
    sem_post(sem);
    return is_first;
}

/**
 * @brief Removes the miner's PID from MINERS_LOG. If it was the last one,
 * destroys the file.
 * @param pid PID of the exiting miner.
 * @param sem Already opened semaphore.
 * @return The number of remaining miners in the system.
 */
static int unregister_miner(pid_t pid, sem_t *sem)
{
    FILE *f;
    char line[64];
    char pid_line[64];
    int remaining = 0;

    snprintf(pid_line, sizeof(pid_line), "Miner PID: %d\n", (int)pid);

    sem_wait(sem);
    f = fopen(MINERS_LOG, "r");
    if (f == NULL)
    {
        sem_post(sem);
        return 0;
    }

    /* Use a temporary file to copy all PIDs except ours */
    FILE *tmp = fopen("miners_tmp.log", "w");
    if (tmp == NULL)
    {
        fclose(f);
        sem_post(sem);
        return 0;
    }

    while (fgets(line, sizeof(line), f) != NULL)
    {
        if (strcmp(line, pid_line) != 0)
        {
            fputs(line, tmp);
            remaining++;
        }
    }

    fclose(f);
    fclose(tmp);

    /* If there are no miners left, clean up the temporary files and the log */
    if (remaining == 0)
    {
        remove("miners_tmp.log");
        remove(MINERS_LOG);
    }
    else
    {
        /* If there are miners left, replace the old log with the updated temporary one */
        rename("miners_tmp.log", MINERS_LOG);
        f = fopen(MINERS_LOG, "r");
        while (fgets(line, sizeof(line), f) != NULL)
        {
            printf(" %s", line);
        }
        fclose(f);
    }
    printf("Miner %d exited system\n", (int)pid);
    sem_post(sem);

    return remaining;
}

/**
 * @brief Verifies the proposed solution and vote in the voting file.
 */
void vote(int target, int solution_proposed, sem_t *sem_votes)
{
    FILE *f = NULL;
    char vote;

    /* Verify if the solution is correct */
    if (pow_hash(solution_proposed) == target)
    {
        /* Introduce a 20% probability of wrong vote */
        if (rand() % 100 < 20)
            vote = 'N';
        else
            vote = 'Y';
    }
    else
    {
        vote = 'N';
    }

    sem_wait(sem_votes);
    f = fopen(VOTING_FILE, "a");
    if (f != NULL)
    {
        fprintf(f, "%c ", vote);
        fclose(f);
    }
    sem_post(sem_votes);
}

/**
 * @brief Counts the number of miners currently registered.
 */
int count_miners(sem_t *sem)
{
    FILE *f;
    char line[64];
    int total = 0;

    sem_wait(sem);
    f = fopen(MINERS_LOG, "r");
    if (f != NULL)
    {
        while (fgets(line, sizeof(line), f) != NULL)
            total++;
        fclose(f);
    }
    sem_post(sem);

    return total;
}

/**
 * @brief  Signal handler.
 * @details Modifies global atomic flags depending on the received signal.
 */
static void handler_yo(int sig)
{
    if (sig == SIGUSR1)
        got_sigusr1 = 1; /* New round started */
    if (sig == SIGUSR2)
        got_sigusr2 = 1; /* Someone found the solution */
    if (sig == SIGALRM)
        time_is_up = 1; /* Alarm expired: lifetime ended */
}

/**
 * @brief The winner counts the votes.
 */
void count_votes(sem_t *sem, sem_t *sem_votes, int *p_yes_votes, int *p_total_votes, char *votes_array)
{
    FILE *f;
    int total_miners = 0;
    int expected_votes = 0;
    int current_votes = 0;
    int yes_votes = 0;
    char v;
    int len;

    int retries = 0;
    int MAX_RETRIES = 50; /* Limit to avoid deadlocks */

    struct timespec req = {0, 100L};

    total_miners = count_miners(sem);
    /* We expect a vote from everyone, except the winner */
    expected_votes = total_miners - 1;

    /* Keep reading until all votes arrive or we exceed the limit */
    while (current_votes < expected_votes && retries < MAX_RETRIES)
    {
        /* Wait of 100 milliseconds */
        nanosleep(&req, NULL);
        retries++;

        current_votes = 0;
        yes_votes = 0;
        votes_array[0] = '\0'; /* Reset the string in each read iteration */

        sem_wait(sem_votes);
        f = fopen(VOTING_FILE, "r");
        if (f != NULL)
        {
            /* Read vote by vote skipping spaces */
            while (fscanf(f, " %c", &v) == 1)
            {
                current_votes++;
                if (v == 'Y')
                    yes_votes++;

                len = strlen(votes_array);
                if (len < 250)
                {
                    votes_array[len] = v;
                    votes_array[len + 1] = ' ';
                    votes_array[len + 2] = '\0';
                }
            }
            fclose(f);
        }
        sem_post(sem_votes);
    }
    /* Assign results to the pointers passed by reference */
    *p_yes_votes = yes_votes;
    *p_total_votes = current_votes;
}

/**
 * @brief Main execution function.
 * @details Validates inputs, spawns the Logger process via fork(), creates
 * the pipelines, manages the multithreaded rounds of the Miner, and sends
 * results to the Comprobador via a POSIX message queue.
 */
int main(int argc, char *argv[])
{
    pid_t pid;
    int i, j, n_secs, n_threads, range, found_solution, target, my_coins = 0;
    int id_round = 0;
    int remaining_miners;
    int is_winner;
    pthread_t *threads = NULL;
    ThreadArgs *args = NULL;

    int flag_first_miner;
    int pipe1_status, pipe2_status;
    ssize_t nbytes;
    int fd1[2], fd2[2];
    char read_buffer[MAX_SIZE];

    int log_fd;
    int auxPID;
    char filename[50];
    char line[64];
    int solucion_ganador;

    FILE *f = NULL;
    MessagePipeline message;

    /* Message queue descriptor and block */
    mqd_t mq;
    MQBlock mq_block;

    sem_t *sem = NULL;
    sem_t *sem_target = NULL;
    sem_t *sem_winner = NULL;
    sem_t *sem_votes = NULL;

    struct sigaction act;
    sigset_t wait_mask, block_mask_SIG1, block_mask_SIG2;

    struct timespec req = {0, 100L};

    if (argc != 3)
    {
        fprintf(stderr, "./%s <N_SECS> <N_THREADS>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    srand(time(NULL));
    n_secs = atoi(argv[1]);
    n_threads = atoi(argv[2]);

    if (n_threads <= 0 || n_secs <= 0)
    {
        printf("Input error\n");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    act.sa_flags = SA_RESTART;
    sigemptyset(&act.sa_mask);
    act.sa_handler = handler_yo;

    if (sigaction(SIGUSR1, &act, NULL) == -1)
    {
        perror("SIGUSR1");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }
    if (sigaction(SIGUSR2, &act, NULL) == -1)
    {
        perror("SIGUSR2");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }
    if (sigaction(SIGALRM, &act, NULL) == -1)
    {
        perror("SIGALRM");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    /* Initialize semaphores */
    sem = sem_open(SEM_NAME, O_CREAT, 0644, 1);
    sem_target = sem_open(SEM_TARGET, O_CREAT, 0644, 1);
    sem_winner = sem_open(SEM_WINNER, O_CREAT, 0644, 1);
    sem_votes = sem_open(SEM_VOTES, O_CREAT, 0644, 1);

    if (sem == SEM_FAILED || sem_target == SEM_FAILED || sem_winner == SEM_FAILED || sem_votes == SEM_FAILED)
    {
        perror("sem_open error");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    /*
     * Try to open the message queue WITHOUT O_CREAT.
     * The monitor (Comprobador) is responsible for creating it.
     * If the queue does not exist, monitor is not running: error and exit.
     */
    mq = mq_open(MQ_NAME, O_WRONLY);
    if (mq == (mqd_t)-1)
    {
        fprintf(stderr, "Error: Monitor is not running. Please start monitor first.\n");
        printf("Miner exited unexpectedly\n");
        sem_close(sem);
        sem_close(sem_target);
        sem_close(sem_winner);
        sem_close(sem_votes);
        exit(EXIT_FAILURE);
    }

    /* Register the miner and indicate if it is the first */
    flag_first_miner = register_miner(getpid(), sem);

    /* Dynamic memory allocation for the threads */
    threads = (pthread_t *)calloc(n_threads, sizeof(pthread_t));
    args = (ThreadArgs *)calloc(n_threads, sizeof(ThreadArgs));
    if (threads == NULL || args == NULL)
    {
        perror("calloc error");
        printf("Miner exited unexpectedly\n");
        mq_close(mq);
        exit(EXIT_FAILURE);
    }

    /* Distribute the search range among the number of threads */
    range = POW_LIMIT / n_threads;
    for (i = 0; i < n_threads; i++)
    {
        args[i].start = i * range;
        args[i].end = (i + 1) * range;
        args[i].solution = &found_solution;
    }
    args[n_threads - 1].end = POW_LIMIT; /* Adjustment to not leave unexplored space in the last thread */

    /* Create pipes for the Miner (Parent) to talk to the Logger (Child) */
    pipe1_status = pipe(fd1);
    pipe2_status = pipe(fd2);
    if (pipe1_status == -1 || pipe2_status == -1)
    {
        perror("pipe error");
        printf("Miner exited unexpectedly\n");
        mq_close(mq);
        exit(EXIT_FAILURE);
    }

    pid = fork();
    if (pid < 0)
    {
        perror("Error in the Fork");
        printf("Miner exited unexpectedly\n");
        mq_close(mq);
        exit(EXIT_FAILURE);
    }
    /* CHILD PROCESS (LOGGER / REGISTRADOR) */
    else if (pid == 0)
    {
        /* Close the pipe ends not used by the child */
        close(fd1[1]);
        close(fd2[0]);

        /* The logger does not need semaphores */
        sem_close(sem);
        sem_close(sem_target);
        sem_close(sem_winner);
        sem_close(sem_votes);

        /* The logger does not need the message queue */
        mq_close(mq);

        snprintf(filename, sizeof(filename), "%d.txt", getppid());
        log_fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log_fd == -1)
        {
            perror("Error open .log");
            exit(EXIT_FAILURE);
        }

        while (read(fd1[0], &message, sizeof(MessagePipeline)) > 0)
        {
            dprintf(log_fd, "Id: %d\n", message.id_round);
            dprintf(log_fd, "Winner: %d\n", getppid());
            dprintf(log_fd, "Target: %08d\n", message.target);

            if (message.is_valid)
            {
                dprintf(log_fd, "Solution: %08d (validated)\n", message.solucion);
            }
            else
            {
                dprintf(log_fd, "Solution: %08d (rejected)\n", message.solucion);
            }

            dprintf(log_fd, "Votes: %d/%d\n", message.votes_yes, message.votes_total);
            /* Use actual coin count instead of a placeholder PID */
            dprintf(log_fd, "Wallets: %d:%d\n\n", getppid(), message.coins);

            /* Send confirmation to the miner */
            write(fd2[1], "OK", 3);
        }
        close(log_fd);
        close(fd1[0]);
        close(fd2[1]);
        free(threads);
        free(args);
        exit(EXIT_SUCCESS);
    }
    /* PARENT PROCESS (MINER)*/
    else
    {
        /* Close pipe ends not used by the parent */
        close(fd1[0]);
        close(fd2[1]);

        /* Configure masks to protect sigsuspend entry */
        sigemptyset(&wait_mask);

        sigemptyset(&block_mask_SIG1);
        sigaddset(&block_mask_SIG1, SIGUSR1);
        sigemptyset(&block_mask_SIG2);
        sigaddset(&block_mask_SIG2, SIGUSR2);

        /* Exactly in n_secs, we send SIGALRM to this process */
        alarm(n_secs);

        /* The first miner entering the system */
        if (flag_first_miner)
        {
            while (count_miners(sem) < 2 && !time_is_up)
            {
                struct timespec req = {0, 100L};
                nanosleep(&req, NULL);
            }

            if (!time_is_up)
            {
                sem_wait(sem_target);
                f = fopen(TARGET_FILE, "w");
                target = 0;
                fprintf(f, "%d\n", target);
                fclose(f);
                sem_post(sem_target);

                /* Send signal SIGUSR1 */
                sem_wait(sem);
                f = fopen(MINERS_LOG, "r");
                if (f != NULL)
                {
                    while (fgets(line, sizeof(line), f) != NULL)
                        if (sscanf(line, "Miner PID: %d", &auxPID) == 1)
                            kill(auxPID, SIGUSR1);
                    fclose(f);
                }
                sem_post(sem);
                /* We set our own flag to start */
                got_sigusr1 = 1;
            }
        }

        while (!time_is_up)
        {
            /* If we are left alone, pause mining */
            if (count_miners(sem) < 2)
            {
                nanosleep(&req, NULL);
                continue;
            }

            /*Temporarily block SIGUSR1. If the signal arrives right at this microsecond
             * fraction, it will be "pending" and sigsuspend will catch it immediately.*/
            sigprocmask(SIG_BLOCK, &block_mask_SIG1, NULL);
            while (got_sigusr1 == 0 && !time_is_up)
            {
                sigsuspend(&wait_mask);
            }
            sigprocmask(SIG_UNBLOCK, &block_mask_SIG1, NULL);

            /* If SIGALRM woke us instead of SIGUSR1, break the loop*/
            if (time_is_up)
                break;

            got_sigusr1 = 0;

            sem_wait(sem_target);
            f = fopen(TARGET_FILE, "r");
            if (f != NULL)
            {
                fscanf(f, "%d", &target);
                fclose(f);
            }
            sem_post(sem_target);

            id_round++;
            found_solution = NO_SOLUTION;
            is_winner = 0;

            for (j = 0; j < n_threads; j++)
            {
                args[j].target = target;
                pthread_create(&threads[j], NULL, pow_func, &args[j]);
            }

            for (j = 0; j < n_threads; j++)
            {
                pthread_join(threads[j], NULL);
            }

            if (time_is_up)
                break;

            /* Evaluate if we found the solution before anyone sent us SIGUSR2 */
            if ((found_solution != NO_SOLUTION) && (got_sigusr2 == 0))
            {
                /* Many could find the solution at the same time. The first to lock
                 * the sem_winner is the winner. */
                if (sem_trywait(sem_winner) == 0)
                {
                    is_winner = 1;

                    sem_wait(sem_votes);
                    f = fopen(VOTING_FILE, "w");
                    if (f != NULL)
                        fclose(f);
                    sem_post(sem_votes);

                    /* The winner inscribes its solution on the target.txt */
                    sem_wait(sem_target);
                    f = fopen(TARGET_FILE, "w");
                    if (f != NULL)
                    {
                        fprintf(f, "%d\n", found_solution);
                        fclose(f);
                    }
                    sem_post(sem_target);

                    /* The winner stops everyone by sending them SIGUSR2 */
                    sem_wait(sem);
                    f = fopen(MINERS_LOG, "r");
                    if (f != NULL)
                    {
                        while (fgets(line, sizeof(line), f) != NULL)
                        {
                            if (sscanf(line, "Miner PID: %d", &auxPID) == 1)
                                if (auxPID != getpid())
                                    kill(auxPID, SIGUSR2);
                        }
                        fclose(f);
                    }
                    sem_post(sem);
                }
            }

            /*The losers*/
            if (is_winner == 0)
            {
                sigprocmask(SIG_BLOCK, &block_mask_SIG2, NULL);
                while (got_sigusr2 == 0 && !time_is_up)
                {
                    sigsuspend(&wait_mask);
                }
                sigprocmask(SIG_UNBLOCK, &block_mask_SIG2, NULL);

                /* If the alarm cut our sleep short, we go home */
                if (time_is_up && got_sigusr2 == 0)
                    break;

                got_sigusr2 = 0;

                solucion_ganador = NO_SOLUTION;
                sem_wait(sem_target);
                f = fopen(TARGET_FILE, "r");
                if (f != NULL)
                {
                    fscanf(f, "%d", &solucion_ganador);
                    fclose(f);
                }
                sem_post(sem_target);

                vote(target, solucion_ganador, sem_votes);
                message.is_valid = 0;
            }
            /*The winner*/
            else
            {
                int yes_votes = 0;
                int current_votes = 0;
                char votes_array[256] = "";

                count_votes(sem, sem_votes, &yes_votes, &current_votes, votes_array);

                if (current_votes == 0)
                {
                    sem_post(sem_winner);
                    continue;
                }
                if (yes_votes >= (current_votes / 2.0))
                {
                    printf("Winner %d => [ %s] => Accepted\n", getpid(), votes_array);
                    message.is_valid = 1;
                    my_coins++;
                }
                else
                {
                    printf("Winner %d => [ %s] => Rejected\n", getpid(), votes_array);

                    message.is_valid = 0;

                    sem_wait(sem_target);
                    f = fopen(TARGET_FILE, "w");
                    if (f != NULL)
                    {
                        fprintf(f, "%d\n", target);
                        fclose(f);
                    }
                    sem_post(sem_target);
                }

                /*
                 * The winner sends target + solution to the Comprobador via the
                 * message queue. The Comprobador will independently validate the
                 * solution and log the result. Only the winning miner sends this block.
                 */
                mq_block.target = target;
                mq_block.solution = found_solution;
                mq_block.is_final = 0;
                if (mq_send(mq, (char *)&mq_block, sizeof(MQBlock), 1) == -1)
                {
                    perror("mq_send");
                }

                message.votes_yes = yes_votes;
                message.votes_total = current_votes;
                message.coins = my_coins;
                message.id_round = id_round;
                message.solucion = found_solution;
                message.target = target;

                nbytes = write(fd1[1], &message, sizeof(MessagePipeline));
                if (nbytes == -1)
                    exit(EXIT_FAILURE);

                read(fd2[0], read_buffer, MAX_SIZE);
                sem_post(sem_winner);

                /*New round */
                sem_wait(sem);
                f = fopen(MINERS_LOG, "r");
                if (f != NULL)
                {
                    while (fgets(line, sizeof(line), f) != NULL)
                        if (sscanf(line, "Miner PID: %d", &auxPID) == 1)
                            kill(auxPID, SIGUSR1);
                    fclose(f);
                }
                sem_post(sem);
            }
            got_sigusr2 = 0;
        }

        close(fd1[1]);
        close(fd2[0]);

        remaining_miners = unregister_miner(getpid(), sem);

        sem_close(sem);
        sem_close(sem_target);
        sem_close(sem_winner);
        sem_close(sem_votes);

        if (remaining_miners == 0)
        {
            sem_unlink(SEM_NAME);
            sem_unlink(SEM_TARGET);
            sem_unlink(SEM_WINNER);
            sem_unlink(SEM_VOTES);

            /*
             * The last miner sends a special termination block to the Comprobador
             * so it knows the system is shutting down. The miner does NOT unlink
             * the queue — that is the Comprobador's responsibility.
             */
            memset(&mq_block, 0, sizeof(MQBlock));
            mq_block.is_final = 1;
            if (mq_send(mq, (char *)&mq_block, sizeof(MQBlock), 0) == -1)
            {
                perror("mq_send (termination block)");
            }
        }

        mq_close(mq);

        free(threads);
        free(args);

        exit(EXIT_SUCCESS);
    }
    return 0;
}