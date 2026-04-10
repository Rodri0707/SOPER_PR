/**
 * @file miner.c
 * @brief Single multi-threaded miner and multi-process logger system.
 * Solves Proof of Work (POW) rounds by brute force using
 * pthreads and communication via pipes.
 * * @details Libraries and Modules:
 * - Standard C Libraries: stdio.h, stdlib.h, string.h, time.h
 * - POSIX Libraries: sys/types.h, sys/wait.h, unistd.h, fcntl.h, pthread.h
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
#include "pow.h"

#define NO_SOLUTION -1
#define MAX_SIZE 20
#define SEM_NAME "/miners_mutex"
#define SEM_TARGET "/target_mutex"
#define SEM_WINNER "/winner_mutex"
#define SEM_VOTES "/vote_mutex"

#define MINERS_LOG "miners.log"  /* File with system info */
#define TARGET_FILE "target.tgt" /* File with the target */
#define VOTING_FILE "voting.vot" /* File with the votes */

/**
 * @struct ThreadArgs
 * @brief Stores the necessary arguments for each mining thread.
 */
typedef struct
{
    int target;    /* Target to search*/
    int start;     /* Starting on: */
    int end;       /* Going on until: */
    int *solution; /* Store solution */
} ThreadArgs;

/**
 * @struct MessagePipeline
 * @brief Data packet sent from the Miner to the Logger through the pipe.
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

/* Global flag to know if we have received SIGUSR1 and SIGUSR2*/
static volatile sig_atomic_t got_sigusr1 = 0;
static volatile sig_atomic_t got_sigusr2 = 0;

/**
 * @brief Routine executed by each thread to search for the target by brute force.
 * @param arg Pointer to the ThreadArgs structure containing the search limits.
 * @return NULL upon completion.
 */
void *pow_func(void *arg)
{
    ThreadArgs *args;
    int i;
    args = (ThreadArgs *)arg;

    for (i = args->start; i < args->end; i++)
    {
        /*If someone discoverd the solition exit*/
        if (got_sigusr2 == 1)
        {
            pthread_exit(NULL);
        }

        /* Check if other thread has already found the solution*/
        if (*(args->solution) != NO_SOLUTION)
        {
            pthread_exit(NULL);
        }

        /* Check if it is the solution */
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
 * Uses a POSIX named semaphore as a mutex to protect the file.
 * @param pid PID to register.
 * @param sem Already-opened semaphore (sem_t *).
 */
static int register_miner(pid_t pid, sem_t *sem)
{
    FILE *f;
    char line[64];
    int is_first = 0;

    /*Critical seccion*/
    sem_wait(sem);

    /*See if this is the first miner*/
    f = fopen(MINERS_LOG, "r");
    if (f == NULL)
    {
        is_first = 1;
    }
    else
    {
        fclose(f);
    }

    /* Write PID to the file */
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
 * destroys the file. Protected by the semaphore.
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

    /*Critical section*/
    sem_wait(sem);

    /* Read the file and rewrite it without this PID's line */
    f = fopen(MINERS_LOG, "r");
    if (f == NULL)
    {
        sem_post(sem);
        return 0;
    }

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

    if (remaining == 0)
    {
        remove("miners_tmp.log");
        remove(MINERS_LOG);
    }
    else
    {
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

void vote(int target, int solution_proposed, sem_t *sem_votes)
{
    FILE *f = NULL;
    char vote;

    if (pow_hash(solution_proposed) == target)
    {
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

static void handler_yo(int sig)
{
    if (sig == SIGUSR1)
    {
        got_sigusr1 = 1;
    }
    if (sig == SIGUSR2)
    {
        got_sigusr2 = 1;
    }
}

/**
 * @brief El ganador recuenta los votos de la urna, esperando a que todos voten.
 * @param sem Semáforo de la lista de mineros (para saber cuántos somos).
 * @param sem_votes Semáforo de la urna de votos.
 * @param p_yes_votes Puntero para devolver el número de votos 'Y'.
 * @param p_total_votes Puntero para devolver el total de votos esperados.
 * @param votes_array Cadena de texto para dibujar los votos (ej: "Y N Y ").
 */
void count_votes(sem_t *sem, sem_t *sem_votes, int *p_yes_votes, int *p_total_votes, char *votes_array)
{
    FILE *f;
    char line[64];
    int total_miners = 0;
    int expected_votes = 0;
    int current_votes = 0;
    int yes_votes = 0;

    int retries = 0;      /* NUEVO: Contador de las veces que hemos mirado la urna */
    int MAX_RETRIES = 50; /* NUEVO: Límite máximo (50 veces * 0.1s = 5 segundos esperando) */

    /* 1. Pasamos lista para saber cuántos mineros somos */
    sem_wait(sem);
    f = fopen(MINERS_LOG, "r");
    if (f != NULL)
    {
        while (fgets(line, sizeof(line), f) != NULL)
            total_miners++;
        fclose(f);
    }
    sem_post(sem);

    /* Esperamos un voto de todos, menos de nosotros mismos */
    expected_votes = total_miners - 1;

    /* 2. Nos quedamos en bucle leyendo la urna hasta que estén todos los votos */
    while (current_votes < expected_votes && retries < MAX_RETRIES)
    {
        struct timespec req = {0, 100000000L};
        nanosleep(&req, NULL);
        retries++;

        current_votes = 0;
        yes_votes = 0;
        votes_array[0] = '\0'; /* Limpiamos la cadena en cada lectura */

        sem_wait(sem_votes);
        f = fopen(VOTING_FILE, "r");
        if (f != NULL)
        {
            char v;
            /* Leemos letra a letra separada por espacios */
            while (fscanf(f, " %c", &v) == 1)
            {
                current_votes++;
                if (v == 'Y')
                    yes_votes++;

                /* Añadimos el voto a la cadena de texto para imprimirlo luego */
                int len = strlen(votes_array);
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

    /* Devolvemos los resultados a través de los punteros */
    *p_yes_votes = yes_votes;
    *p_total_votes = current_votes;
}

/**
 * @brief Main execution function.
 * @details Validates inputs, spawns the Logger process via fork(), creates
 * the pipelines, and manages the multithreaded rounds of the Miner.
 * @param argc Number of arguments.
 * @param argv Array of arguments (<TARGET_INI> <ROUNDS> <N_THREADS>).
 * @return 0 on success, exits with EXIT_FAILURE otherwise.
 */
int main(int argc, char *argv[])
{
    pid_t pid;
    int i, j, n_secs, n_threads, range, found_solution, target;
    int id_round = 0;
    int remaining_miners;
    int is_winner;
    pthread_t *threads = NULL;
    ThreadArgs *args = NULL;
    int my_coins = 0;

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
    struct timespec t_start, t_now;
    double tiempoPasado = 0;

    sem_t *sem = NULL;
    sem_t *sem_target = NULL;
    sem_t *sem_winner = NULL;
    sem_t *sem_votes = NULL;

    struct sigaction act;
    sigset_t wait_mask, block_mask_SIG1, block_mask_SIG2;

    if (argc != 3)
    {
        fprintf(stderr, "./%s <N_SECS> <N_THREADS>\n", argv[0]);
        printf("Miner exited with status %d\n", EXIT_FAILURE);
        exit(EXIT_FAILURE);
    }

    srand(time(NULL));
    n_secs = atoi(argv[1]);
    n_threads = atoi(argv[2]);

    if (n_threads <= 0 || n_secs < 0)
    {
        printf("Input error\n");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    /*Signals*/
    act.sa_flags = 0;
    /*Empty de mask*/
    sigemptyset(&act.sa_mask);
    act.sa_handler = handler_yo;

    if (sigaction(SIGUSR1, &act, NULL) == -1)
    {
        perror("Error when initialize SIGUSR1");
        exit(EXIT_FAILURE);
    }

    if (sigaction(SIGUSR2, &act, NULL) == -1)
    {
        perror("Error when initialize SIGUSR2");
        exit(EXIT_FAILURE);
    }

    /*Initialize sems*/
    sem = sem_open(SEM_NAME, O_CREAT, 0644, 1);
    if (sem == SEM_FAILED)
    {
        perror("sem_open");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    sem_target = sem_open(SEM_TARGET, O_CREAT, 0644, 1);
    if (sem_target == SEM_FAILED)
    {
        perror("sem_open");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    sem_winner = sem_open(SEM_WINNER, O_CREAT, 0644, 1);
    if (sem_winner == SEM_FAILED)
    {
        perror("sem_open");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    sem_votes = sem_open(SEM_VOTES, O_CREAT, 0644, 1);
    if (sem_votes == SEM_FAILED)
    {
        perror("sem_open");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    /* Register miner PID in miners.log */
    flag_first_miner = register_miner(getpid(), sem);

    /* Dynamic memory allocation for threads and their arguments */
    threads = (pthread_t *)calloc(n_threads, sizeof(pthread_t));
    args = (ThreadArgs *)calloc(n_threads, sizeof(ThreadArgs));
    if (threads == NULL || args == NULL)
    {
        perror("Error in calloc");
        printf("Miner exited unexpectedly\n");
        sem_close(sem);
        exit(EXIT_FAILURE);
    }

    range = POW_LIMIT / n_threads;

    /* Initialize the arguments for each thread */
    for (i = 0; i < n_threads; i++)
    {
        args[i].start = i * range;
        args[i].end = (i + 1) * range;
        args[i].solution = &found_solution;
    }
    args[n_threads - 1].end = POW_LIMIT; /* To make sure that we dont exceed the limit */

    /* Create the first pipeline */
    pipe1_status = pipe(fd1);
    if (pipe1_status == -1)
    {
        perror("pipe");
        printf("Miner exited unexpectedly\n");
        sem_close(sem);
        exit(EXIT_FAILURE);
    }

    /* Create the second pipeline */
    pipe2_status = pipe(fd2);
    if (pipe2_status == -1)
    {
        perror("pipe");
        printf("Miner exited unexpectedly\n");
        sem_close(sem);
        exit(EXIT_FAILURE);
    }

    /* Start the timer to measure the time */
    if (clock_gettime(CLOCK_MONOTONIC, &t_start) == -1)
    {
        perror("clock_gettime start");
        sem_close(sem);
        exit(EXIT_FAILURE);
    }

    pid = fork();
    if (pid < 0)
    {
        perror("Error in the Fork");
        printf("Miner exited with status %d\n", EXIT_FAILURE);
        sem_close(sem);
        exit(EXIT_FAILURE);
    }
    /* Logger */
    else if (pid == 0)
    {
        close(fd1[1]);
        close(fd2[0]);

        /* The child inherits the open semaphore; we close it because it doesn't need access to miners.log */
        sem_close(sem);

        /* Get the name of the .log */
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
            dprintf(log_fd, "Wallets: %d:%d\n\n", getppid(), message.coins);

            /* Send confimation to the miner */
            write(fd2[1], "OK", 3);
        }

        close(log_fd);
        close(fd1[0]);
        close(fd2[1]);
        free(threads);
        free(args);
        exit(EXIT_SUCCESS);
    }
    /* Miner */
    else
    {
        close(fd1[0]);
        close(fd2[1]);

        /* Initialize signal masks */
        sigemptyset(&wait_mask); /* Empty mask: allow all signals while suspended */

        sigemptyset(&block_mask_SIG1);
        sigaddset(&block_mask_SIG1, SIGUSR1); /* Block SIGUSR1 temporarily */

        sigemptyset(&block_mask_SIG2);
        sigaddset(&block_mask_SIG2, SIGUSR2); /* Block SIGUSR2 temporarily */

        /* The first miner creates the initial target file */
        if (flag_first_miner)
        {
            while (count_miners(sem) < 2)
            {
                clock_gettime(CLOCK_MONOTONIC, &t_now);
                tiempoPasado = (t_now.tv_sec - t_start.tv_sec) +
                               (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
                if (tiempoPasado >= (double)n_secs)
                    break;

                struct timespec req = {0, 100000000L};
                nanosleep(&req, NULL);
            }

            f = fopen(TARGET_FILE, "w");
            if (f == NULL)
            {
                perror("Error opening " TARGET_FILE);
                exit(EXIT_FAILURE);
            }
            target = 0;
            fprintf(f, "%d\n", target);
            fclose(f);

            /* Notify all registered miners that the first target is ready */
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

            /* The first miner also activates itself */
            got_sigusr1 = 1;
        }

        /* Main mining loop */
        while (tiempoPasado < (double)n_secs)
        {
            if (count_miners(sem) < 2)
            {
                clock_gettime(CLOCK_MONOTONIC, &t_now);
                tiempoPasado = (t_now.tv_sec - t_start.tv_sec) +
                               (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
                struct timespec req = {0, 100000000L};
                nanosleep(&req, NULL);
                continue;
            }
            /* Block SIGUSR1 to avoid race conditions before waiting */
            sigprocmask(SIG_BLOCK, &block_mask_SIG1, NULL);

            /* Wait until SIGUSR1 is received */
            while (got_sigusr1 == 0)
            {
                sigsuspend(&wait_mask); /* Suspend safely with signals enabled */
            }

            /* Reset signal flag for the next round */
            got_sigusr1 = 0;

            /* Unblock SIGUSR1 after waking up */
            sigprocmask(SIG_UNBLOCK, &block_mask_SIG1, NULL);

            /* Read the current target safely */
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

            /* Loop to creat all the threads */
            for (j = 0; j < n_threads; j++)
            {
                args[j].target = target;
                if (pthread_create(&threads[j], NULL, pow_func, &args[j]) != 0)
                {
                    fprintf(stderr, "Error: pthread_create failed\n");
                    printf("Miner exited unexpectedly\n");
                    exit(EXIT_FAILURE);
                }
            }

            /* Loop to wait every thread to finish and continue with next round */
            for (j = 0; j < n_threads; j++)
            {
                pthread_join(threads[j], NULL);
            }

            /*The solution is found*/
            if ((found_solution != NO_SOLUTION) && (got_sigusr2 == 0))
            {
                /*Try to be the first one*/
                if (sem_trywait(sem_winner) == 0)
                {
                    is_winner = 1;

                    /*Clean de Voting file*/
                    sem_wait(sem_votes);
                    f = fopen(VOTING_FILE, "w");
                    if (f != NULL)
                    {
                        fclose(f);
                    }
                    sem_post(sem_votes);

                    sem_wait(sem_target);
                    f = fopen(TARGET_FILE, "w");
                    if (f != NULL)
                    {
                        fprintf(f, "%d\n", found_solution);
                        fclose(f);
                    }
                    sem_post(sem_target);

                    sem_wait(sem);
                    f = fopen(MINERS_LOG, "r");
                    if (f != NULL)
                    {
                        while (fgets(line, sizeof(line), f) != NULL)
                        {
                            if (sscanf(line, "Miner PID: %d", &auxPID) == 1)
                            {
                                /*We dont throw the signal to the process who sent */
                                if (auxPID != getpid())
                                {
                                    kill(auxPID, SIGUSR2);
                                }
                            }
                        }
                        fclose(f);
                    }
                    sem_post(sem);
                }
            }

            /*The miners losers*/
            if (is_winner == 0)
            {
                sigprocmask(SIG_BLOCK, &block_mask_SIG2, NULL);
                /* Wait until SIGUSR2 is received */
                while (got_sigusr2 == 0)
                {
                    sigsuspend(&wait_mask); /* Suspend safely with signals enabled */
                }
                got_sigusr2 = 0;

                /* Unblock SIGUSR2 after waking up */
                sigprocmask(SIG_UNBLOCK, &block_mask_SIG2, NULL);

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
            else
            {
                int yes_votes = 0;
                int current_votes = 0;
                char votes_array[256] = "";

                count_votes(sem, sem_votes, &yes_votes, &current_votes, votes_array);
                if (current_votes == 0)
                {
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

                /*New round*/
                sem_wait(sem);
                f = fopen(MINERS_LOG, "r");
                if (f != NULL)
                {
                    while (fgets(line, sizeof(line), f) != NULL)
                    {
                        if (sscanf(line, "Miner PID: %d", &auxPID) == 1)
                            kill(auxPID, SIGUSR1);
                    }
                    fclose(f);
                }
                sem_post(sem);
            }

            got_sigusr2 = 0;

            clock_gettime(CLOCK_MONOTONIC, &t_now);
            tiempoPasado = (t_now.tv_sec - t_start.tv_sec) +
                           (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
        }

        close(fd1[1]);
        close(fd2[0]);

        /*Unregister the miner: remove PID and show status */
        remaining_miners = unregister_miner(getpid(), sem);

        /* Clean up semaphore */
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
        }

        free(threads);
        free(args);
        exit(EXIT_SUCCESS);
    }

    return 0;
}