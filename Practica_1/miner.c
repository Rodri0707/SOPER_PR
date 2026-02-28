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

typedef struct
{
    int target;    /* Target to search*/
    int start;     /* Starting on: */
    int end;       /* Going on until: */
    int *solution; /* Store solution */
} ThreadArgs;

typedef struct
{
    int id_round;
    int target;
    int solucion;
    int is_valid; /* 1 si es validada, 0 si es rechazada */
} MessagePipeline;

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

    threads = (pthread_t *)calloc(n_threads, sizeof(pthread_t));
    args = (ThreadArgs *)calloc(n_threads, sizeof(ThreadArgs));
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
        printf("Miner exited unexpectedly");
        exit(EXIT_FAILURE);
    }
    /*Create the second pipeline*/
    pipe2_status = pipe(fd2);
    if (pipe2_status == -1)
    {
        perror("pipe");
        printf("Miner exited unexpectedly");
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
                pthread_create(&threads[j], NULL, pow_func, &args[j]);
            }
            /* Loop to wait every thread to finish and continue with next round */
            for (j = 0; j < n_threads; j++)
            {
                pthread_join(threads[j], NULL);
            }

            message.id_round = i+1;
            message.is_valid = (rand() % 10 !=0); /*10% of no valid*/
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

        free(threads);
        free(args);
        exit(EXIT_SUCCESS);
    }
    return 0;
}