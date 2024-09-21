#ifndef WORKER_H
#define WORKER_H

#include <Python.h>
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <json-c/json.h>

#define MAX_URL_LENGTH 256
#define RETRY_DELAY 5
#define TASK_DELAY 1
#define DEFAULT_TASK_TYPE "ip_data"
/* 
 * Structure to hold memory for CURL write callback: 
 * @memory: A pointer to dynamically allocated memory, 
 * @size: The current size of allocated memory 
 * */
typedef struct MemoryStruct {
    char *memory;
    size_t size;
} MemoryStruct;

/* Function prototypes */
static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp);
static void handle_curl_error(CURL *curl, CURLcode res);
static PyObject *setup_python_config(void);
static PyObject *setup_task_processor(PyObject *pConfigInstance);
static PyObject *setup_asyncio(void);
static int parse_task_json(const char *task, json_object **task_content,
                           json_object **ip_address, json_object **task_id,
                           json_object **client_ip, json_object **task_type);
static PyObject *create_python_task_dict(json_object *ip_address,
                                         json_object *task_id,
                                         json_object *client_ip,
                                         json_object *task_type);
static int process_task(PyObject *pFunc, json_object *task);
static int request_and_process_task(CURL *curl, const char *host, int port,
                                    const char *worker_id, MemoryStruct *chunk,
                                    PyObject *pFunc);
static json_object *parse_json(const char *response);
static int process_json_task(json_object *task_obj, PyObject *pFunc);
static int reallocate_chunk_memory(MemoryStruct *chunk);

PyObject *init_python(void);
int init(CURL **curl, PyObject **pFunc, MemoryStruct *chunk);
int request_task(CURL *curl, const char *host, int port, const char *worker_id, MemoryStruct *chunk, const char *task_type);
char *process_tasks(PyObject *pFuncTuple, const char *task);
int submit_result(CURL *curl, const char *host, int port, const char *worker_id, const char *result_str, const char *task_type);
int main(int argc, char *argv[]);

#endif
