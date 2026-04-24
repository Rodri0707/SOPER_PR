#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>       
#include <fcntl.h>           
#include <unistd.h>   
#include <sys/wait.h>   


void creacionRecursos(){

}

void limpiezaRecursos(){

}

int main(int argc, char *argv[]){

    int comprobacion_lag;
    int monitor_lag;
    int fin_sistema;
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
        printf("Input error: Los retardos deben de ser mayores que 0\\n");
        printf("El monitor se cerró\n");
        exit(EXIT_FAILURE);
    }

    creacionRecursos();

    pid = fork();
    if (pid < 0)
    {
        perror("Error en el Fork");
        printf("El monitor se cerró\n");
        limpiezaRecursos();
        exit(EXIT_FAILURE);
    }
    /* MONITOR */
    else if(pid == 0){
        fin_sistema = 0;
        while(!fin_sistema){

            usleep(monitor_lag * 1000);

            fin_sistema = 1;
        }
        exit(EXIT_SUCCESS);
    }
    /* COMPROBADOR*/
    else{
        fin_sistema = 0;
        while(!fin_sistema){

            usleep(comprobacion_lag * 1000);

            fin_sistema = 1;
        }

        wait(NULL);
        limpiezaRecursos();
        exit(EXIT_SUCCESS);
    }
}