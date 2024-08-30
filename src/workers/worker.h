#ifndef WORKER_H
#define WORKER_H

#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>


#define MAX_BUFFER_SIZE 1024
#define DEFAULT_NUM_WORKERS 1
#define CONFIG_FILE_PATH "config/processor_config.ini"

typedef struct {
    char *host;
    int port;
} worker_args;


void init_python(void);
void finalize_python(void);
PyObject* process_task(PyObject* task);
PyObject *pModule, *pFunc;
void run_worker(const char* host, int port);
void* worker_thread(void* arg);
int get_num_workers(void);

#endif