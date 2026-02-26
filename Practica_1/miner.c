#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

int main(int argc, char *argv[])
{
    pid_t pid;
    int status;

    if (argc != 4)
    {
        fprintf(stderr, "./%s <TARGET_INI> <ROUNDS> > <N_THREADS>\n", argv[0]);
        printf("Miner exited with status %d\n", EXIT_FAILURE);
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
        printf("REGISTRADOR: Hola que tal estamos\n");
        exit(EXIT_SUCCESS);
    }
    else /*Proceso del minero (Padre)*/
    {
        printf("MINERO: Hola mi brodi, SALUD Y REPUBLICA!\n");
        if (waitpid(pid, &status, 0) == -1)
        {
            perror("Error en waitpid");
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
        exit(EXIT_SUCCESS); 
    }

    return 0;
}