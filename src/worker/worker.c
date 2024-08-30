#include "worker.h"

void init_python(void)
{
    Py_Initialize();
    PyRun_SimpleString("import sys; sys.path.append('.')");
    pModule = PyImport_ImportModule("src.python.task_processor");
    if (pModule == NULL)
    {
        PyErr_Print();
        exit(EXIT_FAILURE);
    }

    pFunc = PyObject_GetAttrString(pModule, "process_tasks");
    if (pFunc == NULL)
    {
        PyErr_Print();
        exit(EXIT_FAILURE);
    } 
}

void finalize_python(void)
{
    Py_XDECREF(pFunc);
    Py_XDECREF(pModule);
    Py_Finalize();
}

PyObject* process_task(PyObject* task)
{
    PyObject *pArgs, *pValue;

    pArgs = PyTuple_New(1);
    PyTuple_SetItem(pArgs, 0, task);
    pValue = PyObject_CallObject(pFunc, pArgs);
    Py_DECREF(pArgs);

    if (pValue == NULL)
    {
        PyErr_Print();
        return NULL;
    }
    return pValue;
}

void run_worker(const char* host, int port)
{
    int sock = 0;
    struct sockaddr_in serv_addr;
    char buffer[MAX_BUFFER_SIZE] = {0};
    int valread;

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        perror("\n Socket creation error \n");
        return;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0 )
    {
        perror("\nInvalid address/ Address not supported \n");
        return;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("\nConnection Failed \n");
        return;
    }

    while (1)
    {
        valread = read(sock, buffer, MAX_BUFFER_SIZE);
        if (valread > 0)
        {
            PyObject *pTask = PyUnicode_FromString(buffer);
            PyObject *pResult = process_task(pTask);
            Py_DECREF(pTask);

            if (pResult != NULL)
            {
                const char* result_str = PyUnicode_AsUTF8(pResult);
                if (send(sock, result_str, strlen(result_str), 0) < 0)
                {
                    perror("Failed to send result");
                }
                Py_DECREF(pResult);
            }
        }
    }

    close(sock);
}

void* worker_thread(void* arg)
{
    worker_args* args = (worker_args*)arg;
    run_worker(args->host, args->port);
    return NULL;
}

int get_num_workers(void)
{
    FILE* config_file;
    char line[256];
    int num_workers = DEFAULT_NUM_WORKERS;

    config_file = fopen(CONFIG_FILE_PATH, 'r');
    if (config_file == NULL)
    {
        perror("Error opening the configuration file");
        return DEFAULT_NUM_WORKERS;
    }

    while (fgets(line, sizeof(line), config_file))
    {
        if (sscanf(line, "num_workers = %d"))
    }
}
int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s <host> <port>\n", argv[0]);
        exit(1);
    }

    FILE* config_file = fopen("config/processor_config.ini");
    if (config_file == NULL)
    {
        perror("Error opening configuration file");
        exit(1);
    }

    char line[256];
    int num_workers = 1;
    while (fgets(line, sizeof(line), config_file))
    {
        if (sscanf(line, "num_workers = %d", &num_workers) == 1)
        {
            break;
        }
    }

    fclose(config_file);
    init_python();

    pthread_t threads[num_workers];
    worker_args args = {argv[1], atoi(argv[2])};
    int i;
    
    for (i = 0; i < num_workers; i++)
    {
        pthread_create(&threads[i], NULL, worker_thread, (void*)&args);
    }

    int i;
    for (i = 0; i < num_workers; i++)
    {
        pthread_join(threads[i], NULL);
    }

    finalize_python();
    return 0;
}