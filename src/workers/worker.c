#include "worker.h"

void init_python(void)
{
    Py_Initialize();
    PyRun_SimpleString("import sys; sys.path.append('.')");

    PyObject* pConfigModule = PyImport_ImportModule("src.python.config");
    if (pConfigModule == NULL)
    {
        PyErr_Print();
        exit(EXIT_FAILURE);
    }

    PyObject* pConfigClass = PyObject_GetAttrString(pConfigModule, "Config");
    if (pConfigClass == NULL)
    {
        PyErr_Print();
        exit(EXIT_FAILURE);
    }

    PyObject* pConfigInstance = PyObject_CallObject(pConfigClass, NULL);
    if (pConfigInstance == NULL)
    {
        PyErr_Print();
        exit(EXIT_FAILURE);
    }

    pModule = PyImport_ImportModule("src.python.task_processor");
    if (pModule == NULL)
    {
        PyErr_Print();
        exit(EXIT_FAILURE);
    }

    PyObject* pClass = PyObject_GetAttrString(pModule, "TaskProcessor");
    if (pClass == NULL)
    {
        PyErr_Print();
        exit(EXIT_FAILURE);
    }

    PyObject* pArgs = PyTuple_New(2);
    PyTuple_SetItem(pArgs, 0, pConfigInstance);
    PyTuple_SetItem(pArgs, 1, Py_True);
    PyObject* pInstance = PyObject_CallObject(pClass, pArgs);
    Py_DECREF(pArgs);
    if (pInstance == NULL)
    {
        PyErr_Print();
        exit(EXIT_FAILURE);
    }

    pFunc = PyObject_GetAttrString(pInstance, "process_tasks_sync");
    if (pFunc == NULL)
    {
        PyErr_Print();
        exit(EXIT_FAILURE);
    }

    Py_DECREF(pConfigClass);
    Py_DECREF(pClass);
    Py_DECREF(pConfigModule);
    Py_DECREF(pInstance);
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

    config_file = fopen(CONFIG_FILE_PATH, "r");
    if (config_file == NULL)
    {
        perror("Error opening the configuration file");
        return DEFAULT_NUM_WORKERS;
    }

    while (fgets(line, sizeof(line), config_file))
    {
        if (sscanf(line, "num_workers = %d", &num_workers) == 1)
        break;
    }
    fclose(config_file);
    return num_workers;
}
int main(int argc, char *argv[])
{
    pthread_t *threads;
    worker_args args;
    int i, num_workers;

    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s <host> <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    num_workers = get_num_workers();

    init_python();

    threads = malloc(num_workers * sizeof(pthread_t));
    if (threads == NULL)
    {
        perror("Failed to create thread");
        exit(EXIT_FAILURE);
    }

    args.host = argv[1];
    args.port = atoi(argv[2]);

    for (i = 0; i < num_workers; i++)
    {
        if (pthread_create(&threads[i], NULL, worker_thread, (void *)&args) != 0)
        {
            perror("Failed to create thread");
            exit(EXIT_FAILURE);
        }
    }

    for (i = 0; i < num_workers; i++)
    {
        pthread_join(threads[i], NULL);
    }

    free(threads);
    finalize_python();

    return 0;
}
