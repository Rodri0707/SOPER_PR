#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include "pow.h"
#define NO_SOLUTION -1
#define MAX_SIZE 20
typedef struct {
    int target;     /* Target to search*/
    int start;      /* Starting on: */
    int end;        /* Going on until: */
    int *solution;  /* Store solution */
} ThreadArgs;

void *pow_func(void *arg){
    ThreadArgs *args;
    int i;
    args = (ThreadArgs*)arg;
    for(i = args->start; i<args->end; i++){
        /* Check if other thread has alredy found the solution*/
        if(*(args->solution) != NO_SOLUTION){
            pthread_exit(NULL);
        }

        /* Check if it is the solution */
        if(pow_hash(i) == args->target){
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
    int pipe_status;
    ssize_t nbytes;
    int fd[2];
    char string[MAX_SIZE], read_buffer[MAX_SIZE];
    if (argc != 4)
    {
        fprintf(stderr, "./%s <TARGET_INI> <ROUNDS> <N_THREADS>\n", argv[0]);
        printf("Miner exited with status %d\n", EXIT_FAILURE);
        exit(EXIT_FAILURE);
    }

    target = atoi(argv[1]);
    n_rounds = atoi(argv[2]);
    n_threads = atoi(argv[3]);
    threads = (pthread_t *)calloc(n_threads, sizeof(pthread_t));
    args = (ThreadArgs *)calloc(n_threads, sizeof(ThreadArgs));
    range = POW_LIMIT / n_threads;

    /* Initialize the arguments for each thread*/
    for(i = 0; i<n_threads; i++){
        args[i].start = i*range;
        args[i].end = (i+1)*range;
        args[i].solution = &found_solution;
    }
    args[n_threads-1].end = POW_LIMIT; /* To make sure that we dont exceed the limit */
    
    pipe_status = pipe(fd);
    if(pipe_status == -1){
        perror("pipe");
        printf("Miner exited unexpected");
        exit(EXIT_FAILURE);
    }
    pid = fork();

    if (pid < 0)
    {
        perror("Error in the Fork");
        printf("Miner exited with status %d\n", EXIT_FAILURE);
        exit(EXIT_FAILURE);
    }
    else if (pid == 0) /*Proceso del registrador (hijo)*/
    {
        close(fd[1]);
        printf("REGISTRADOR: Hola que tal estamos\n");
        exit(EXIT_SUCCESS);
    }
    else /*Proceso del minero (Padre)*/
    {
        close(fd[0]);
        /* Loop for all the rounds */
        for(i=0; i<n_rounds; i++){
            found_solution = NO_SOLUTION;
            /* Loop to creat all the threads*/
            for(j=0; j<n_threads; j++){
                args[j].target = target;
                pthread_create(&threads[j], NULL, pow_func, &args[j]);
            }
            /* Loop to wait every thread to finish and continue with next round */
            for(j=0; j<n_threads; j++){
                pthread_join(threads[j], NULL);
            }
            snprintf(string, MAX_SIZE, "%i %i",i, found_solution);
            nbytes = write(fd[1], string, MAX_SIZE);
            if(nbytes == -1){
                perror("Error in pipe");
                printf("Miner exited unexpected\n");
                exit(EXIT_FAILURE);
            }
            target = found_solution;
        }
        close(fd[1]);

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