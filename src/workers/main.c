#include "worker.h"


/**
 * main - Entry point for the worker program
 * @argc: Number of command-line arguments
 * @argv: Array of command-line argument strings
 *
 * Return: 0 on success, 1 on failure
 */
int main(int argc, char *argv[])
{
	char *host, *worker_id;
	int port;
	CURL *curl;
	PyObject *pFunc, *loop, *init_result;
	MemoryStruct chunk;

	if (argc != 4)
	{
		fprintf(stderr, "Usage: %s <host> <port> <worker_id>\n", argv[0]);
		return (1);
	}

	host = argv[1];
	port = atoi(argv[2]);
	worker_id = argv[3];

	init_result = init_python();
	if (!init_result || !PyArg_ParseTuple(init_result, "OO", &pFunc, &loop))
	{
		fprintf(stderr, "Initialization failed\n");
		Py_XDECREF(init_result);
		return (1);
	}

	if (!init(&curl, &pFunc, &chunk))
	{
		fprintf(stderr, "Initialization failed\n");
		Py_DECREF(init_result);
		return (1);
	}

	printf("Initialization successful. Starting main loop...\n");

	while (1)
	{
		if (!request_and_process_task(curl, host, port, worker_id, &chunk, pFunc))
		{
			fprintf(stderr, "Task processing failed. Retrying...\n");
		}
		sleep(TASK_DELAY);
	}

	free(chunk.memory);
	curl_easy_cleanup(curl);
	Py_DECREF(pFunc);
	Py_DECREF(loop);
	Py_DECREF(init_result);
	Py_Finalize();

	return (0);
}

/**
 * process_task - Process a single task using the Python function
 * @pFunc: Python function to process the task
 * @task: JSON object containing the task information
 *
 * Return: 1 on success, 0 on failure
 */
static int process_task(PyObject *pFunc, json_object *task)
{
	const char *task_str, *task_id, *client_ip, *task_type;
	PyObject *pTaskDict, *pTaskList, *result;
	int success = 0;

	task_str = json_object_get_string(json_object_object_get(task, "task"));
	task_id = json_object_get_string(json_object_object_get(task, "task_id"));
	client_ip = json_object_get_string(json_object_object_get(task, "client_ip"));
	task_type = json_object_get_string(json_object_object_get(task, "task_type"));

	if (!task_str || !task_id || !client_ip || !task_type)
	{
		printf("Invalid task structure\n");
		return (0);
	}

	printf("Processing task: %s (ID: %s, Type: %s) from client: %s\n",
	       task_str, task_id, task_type, client_ip);

	pTaskDict = PyDict_New();
	PyDict_SetItemString(pTaskDict, "task", PyUnicode_FromString(task_str));
	PyDict_SetItemString(pTaskDict, "task_id", PyUnicode_FromString(task_id));
	PyDict_SetItemString(pTaskDict, "client_ip", PyUnicode_FromString(client_ip));
	PyDict_SetItemString(pTaskDict, "task_type", PyUnicode_FromString(task_type));

	pTaskList = PyList_New(1);
	PyList_SetItem(pTaskList, 0, pTaskDict);

	result = PyObject_CallFunctionObjArgs(pFunc, pTaskList, NULL);
	Py_DECREF(pTaskList);

	if (result == NULL)
	{
		printf("Failed to process task\n");
		PyErr_Print();
	}
	else
	{
		const char *result_str = PyUnicode_AsUTF8(result);

		printf("Task processed successfully. Result: %s\n", result_str);
		Py_DECREF(result);
		success = 1;
	}

	return (success);
}

/**
 * request_and_process_task - Request a task and process it
 * @curl: CURL handle
 * @host: Host address
 * @port: Port number
 * @worker_id: Worker ID
 * @chunk: Memory structure for storing the response
 * @pFunc: Python function to process the task
 *
 * Return: 1 on success, 0 on failure
 */
static int request_and_process_task(CURL *curl, const char *host, int port,
				    const char *worker_id, MemoryStruct *chunk,
				    PyObject *pFunc)
{
	json_object *task_obj;
	int success = 0;

	if (!reallocate_chunk_memory(chunk))
		return (0);

	if (!request_task(curl, host, port, worker_id, chunk, DEFAULT_TASK_TYPE))
	{
		fprintf(stderr, "Failed to request task. Retrying in %d seconds...\n",
			RETRY_DELAY);
		sleep(RETRY_DELAY);
		return (0);
	}

	task_obj = parse_json(chunk->memory);
	if (task_obj)
	{
		success = process_json_task(task_obj, pFunc);
		json_object_put(task_obj);
	}

	return (success);
}

/**
 * parse_json - Parse JSON response and handle errors
 * @response: JSON response string
 *
 * Return: Parsed JSON object or NULL on failure
 */
static json_object *parse_json(const char *response)
{
	json_object *task_obj = json_tokener_parse(response);

	if (!task_obj)
	{
		fprintf(stderr, "Failed to parse task JSON: %s\n", response);
		return (NULL);
	}

	printf("Parsed JSON: %s\n", json_object_to_json_string(task_obj));
	return (task_obj);
}

/**
 * process_json_task - Process the parsed JSON task
 * @task_obj: Parsed JSON object
 * @pFunc: Python function to process the task
 *
 * Return: 1 on success, 0 on failure
 */
static int process_json_task(json_object *task_obj, PyObject *pFunc)
{
	json_object *task = json_object_object_get(task_obj, "task");

	if (task != NULL)
	{
		return (process_task(pFunc, task));
	}
	else
	{
		printf("No 'task' field found in JSON: %s\n",
		       json_object_to_json_string(task_obj));
		return (0);
	}
}

/**
 * reallocate_chunk_memory - Reallocate memory for chunk
 * @chunk: Memory structure to reallocate
 *
 * Return: 1 on success, 0 on failure
 */
static int reallocate_chunk_memory(MemoryStruct *chunk)
{
	free(chunk->memory);
	chunk->memory = malloc(1);
	chunk->size = 0;

	if (!chunk->memory)
	{
		fprintf(stderr, "Memory allocation failed\n");
		return (0);
	}
	return (1);
}
