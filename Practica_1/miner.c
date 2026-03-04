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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <time.h>
#include "pow.h"

#define NO_SOLUTION -1
#define MAX_SIZE 20

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
    int is_valid; /* 1 is valid, 0 is no valid */
} MessagePipeline;

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
        /* Check if other thread has alredy found the solution*/
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
    int status;
    int i, j, target, n_rounds, n_threads, range, found_solution;
    pthread_t *threads = NULL;
    ThreadArgs *args = NULL;
    int pipe1_status, pipe2_status;
    ssize_t nbytes;
    int fd1[2], fd2[2];
    char read_buffer[MAX_SIZE];
    int log_fd;
    char filename[50];
    MessagePipeline message;
    FILE *f = NULL;
    struct timespec start, end;
    double total_time;

    if (argc != 4)
    {
        fprintf(stderr, "./%s <TARGET_INI> <ROUNDS> <N_THREADS>\n", argv[0]);
        printf("Miner exited with status %d\n", EXIT_FAILURE);
        exit(EXIT_FAILURE);
    }

    srand(time(NULL));
    target = atoi(argv[1]);
    n_rounds = atoi(argv[2]);
    n_threads = atoi(argv[3]);

    if (n_threads <= 0 || n_rounds <= 0 || target < 0 || target >= POW_LIMIT)
    {
        printf("Input error\n");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    /* Dynamic memory allocation for threads and their arguments */
    threads = (pthread_t *)calloc(n_threads, sizeof(pthread_t));
    args = (ThreadArgs *)calloc(n_threads, sizeof(ThreadArgs));
    if (threads == NULL || args == NULL)
    {
        perror("Error in calloc");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }
    range = POW_LIMIT / n_threads;

    /* Initialize the arguments for each thread*/
    for (i = 0; i < n_threads; i++)
    {
        args[i].start = i * range;
        args[i].end = (i + 1) * range;
        args[i].solution = &found_solution;
    }
    args[n_threads - 1].end = POW_LIMIT; /* To make sure that we dont exceed the limit */

    /*Create the first pipeline*/
    pipe1_status = pipe(fd1);
    if (pipe1_status == -1)
    {
        perror("pipe");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }
    /*Create the second pipeline*/
    pipe2_status = pipe(fd2);
    if (pipe2_status == -1)
    {
        perror("pipe");
        printf("Miner exited unexpectedly\n");
        exit(EXIT_FAILURE);
    }

    /* Start the timer to measure the time */
    if (clock_gettime(CLOCK_MONOTONIC, &start) == -1)
    {
        perror("clock_gettime start");
        exit(EXIT_FAILURE);
    }

    pid = fork();
    if (pid < 0)
    {
        perror("Error in the Fork");
        printf("Miner exited with status %d\n", EXIT_FAILURE);
        exit(EXIT_FAILURE);
    }
    /*Registrador*/
    else if (pid == 0)
    {
        close(fd1[1]);
        close(fd2[0]);

        /*Get the name of the .log*/
        snprintf(filename, sizeof(filename), "%d.log", getppid());
        log_fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log_fd == -1)
        {
            perror("Error open .log");
            exit(EXIT_FAILURE);
        }

        while (read(fd1[0], &message, sizeof(MessagePipeline)) > 0)
        {
            dprintf(log_fd, "Id:       %d\n", message.id_round);
            dprintf(log_fd, "Winner:   %d\n", getppid());
            dprintf(log_fd, "Target:   %08d\n", message.target);
            if (message.is_valid)
            {
                dprintf(log_fd, "Solution: %08d (validated)\n", message.solucion);
            }
            else
            {
                dprintf(log_fd, "Solution: %08d (rejected)\n", message.solucion);
            }
            dprintf(log_fd, "Votes:    %d/%d\n", message.id_round, message.id_round);
            dprintf(log_fd, "Wallets:  %d:%d\n\n", getppid(), message.id_round);

            /*Send confimation to the miner */
            write(fd2[1], "OK", 3);
        }
        close(log_fd);
        close(fd1[0]);
        close(fd2[1]);
        free(threads);
        free(args);
        exit(EXIT_SUCCESS);
    }
    /*Miner*/
    else
    {
        close(fd1[0]);
        close(fd2[1]);
        /* Loop for all the rounds */
        for (i = 0; i < n_rounds; i++)
        {
            found_solution = NO_SOLUTION;
            /* Loop to creat all the threads*/
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

            message.id_round = i + 1;
            message.is_valid = (rand() % 10 != 0); /*10% of no valid*/
            message.solucion = found_solution;
            message.target = target;

            /*Send the results to the Registrador*/
            nbytes = write(fd1[1], &message, sizeof(MessagePipeline));
            if (nbytes == -1)
            {
                perror("Error in pipe");
                printf("Miner exited unexpectedly\n");
                exit(EXIT_FAILURE);
            }

            /*The Miner read the response of the Registrador*/
            read(fd2[0], read_buffer, MAX_SIZE);

            if (message.is_valid == 1)
            {
                printf("Solution accepted: %08d--> %08d\n", message.target, message.solucion);
                /*Set the new target to search*/
                target = found_solution;
            }
            else
            {
                printf("Solution rejected: %08d--> %08d\n", message.target, message.solucion);
            }
        }
        close(fd1[1]);
        close(fd2[0]);

        /*Analyze how the Registrador ends*/
        if (waitpid(pid, &status, 0) == -1)
        {
            perror("Error in waitpid");
            printf("Logger exited unexpectedly\n");
        }
        else
        {
            if (WIFEXITED(status))
            {
                printf("Logger exited with status %d\n", WEXITSTATUS(status));
            }
            else
            {
                printf("Logger exited unexpectedly\n");
            }
        }
        printf("Miner exited with status %d\n", EXIT_SUCCESS);

        /* End the timer and pass the results to tiempos.txt */
        if (clock_gettime(CLOCK_MONOTONIC, &end) == -1)
        {
            perror("clock_gettime end");
            exit(EXIT_FAILURE);
        }
        total_time = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
        if (!(f = fopen("tiempos.txt", "a")))
        {
            perror("Error opening tiempos.txt");
        }
        {
            fprintf(f, "%-8i         %-8i          %-8lf\n", n_rounds, n_threads, total_time);
        }
        fclose(f);
        free(threads);
        free(args);
        exit(EXIT_SUCCESS);
    }
    return 0;
}