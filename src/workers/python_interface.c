#include "worker.h"


/**
 * init_python - Initializes the Python environment and
 *              sets up necessary objects
 *
 * Return: A tuple containing the process_tasks function and event loop,
 *         or NULL on failure
 */
PyObject *init_python(void)
{
	PyObject *pConfigInstance, *pFunc, *loop;

	Py_Initialize();
	PyRun_SimpleString("import sys; import os; "
			   "sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))))");

	pConfigInstance = setup_python_config();
	if (!pConfigInstance)
		return (NULL);

	pFunc = setup_task_processor(pConfigInstance);
	if (!pFunc)
	{
		Py_DECREF(pConfigInstance);
		return (NULL);
	}

	loop = setup_asyncio();
	if (!loop)
	{
		Py_DECREF(pFunc);
		Py_DECREF(pConfigInstance);
		return (NULL);
	}

	return (Py_BuildValue("(OO)", pFunc, loop));
}

/**
 * process_tasks - Processes tasks using the Python function
 * @pFuncTuple: Tuple containing the Python function and event loop
 * @task: JSON string containing task information
 *
 * Return: Result string or NULL on failure
 */
char *process_tasks(PyObject *pFuncTuple, const char *task)
{
	PyObject *pFunc, *loop, *pTaskList, *pCoroutine, *pResult;
	json_object *task_obj, *task_content, *ip_address, *task_id,
		*client_ip, *task_type;
	char *result_str = NULL;

	if (!PyArg_ParseTuple(pFuncTuple, "OO", &pFunc, &loop))
	{
		PyErr_Print();
		return (NULL);
	}

	task_obj = json_tokener_parse(task);
	if (!parse_task_json(task, &task_content, &ip_address, &task_id,
			    &client_ip, &task_type))
	{
		json_object_put(task_obj);
		return (NULL);
	}

	pTaskList = PyList_New(1);
	if (!pTaskList)
	{
		PyErr_Print();
		json_object_put(task_obj);
		return (NULL);
	}

	PyObject *pTaskDict = create_python_task_dict(ip_address,
						     task_id,
						     client_ip,
						     task_type);

	if (!pTaskDict)
	{
		Py_DECREF(pTaskList);
		json_object_put(task_obj);
		return (NULL);
	}

	PyList_SetItem(pTaskList, 0, pTaskDict);

	pCoroutine = PyObject_CallFunctionObjArgs(pFunc, pTaskList, NULL);
	Py_DECREF(pTaskList);

	if (!pCoroutine)
	{
		PyErr_Print();
		json_object_put(task_obj);
		return (NULL);
	}

	PyObject *run_coroutine = PyObject_GetAttrString(loop, "run_until_complete");

	if (!run_coroutine)
	{
		PyErr_Print();
		Py_DECREF(pCoroutine);
		json_object_put(task_obj);
		return (NULL);
	}

	pResult = PyObject_CallFunctionObjArgs(run_coroutine, pCoroutine, NULL);
	Py_DECREF(run_coroutine);
	Py_DECREF(pCoroutine);

	if (!pResult)
	{
		PyErr_Print();
		json_object_put(task_obj);
		return (NULL);
	}

	const char *temp_result_str = PyUnicode_AsUTF8(pResult);

	if (temp_result_str)
		result_str = strdup(temp_result_str);

	Py_DECREF(pResult);
	json_object_put(task_obj);
	return (result_str);
}

/**
 * init - Initializes CURL, Python, and memory structures
 * @curl: Pointer to CURL handle
 * @pFunc: Pointer to Python function
 * @chunk: Pointer to memory structure
 *
 * Return: 1 on success, 0 on failure
 */
int init(CURL **curl, PyObject **pFunc, MemoryStruct *chunk)
{
	*curl = curl_easy_init();
	if (!*curl)
	{
		fprintf(stderr, "Failed to initialize curl\n");
		return (0);
	}

	*pFunc = init_python();
	if (!*pFunc)
	{
		fprintf(stderr, "Failed to initialize Python\n");
		curl_easy_cleanup(*curl);
		return (0);
	}

	chunk->memory = malloc(1);
	chunk->size = 0;
	if (!chunk->memory)
	{
		fprintf(stderr, "Initial memory allocation failed\n");
		curl_easy_cleanup(*curl);
		Py_DECREF(*pFunc);
		Py_Finalize();
		return (0);
	}

	return (1);
}

/* Helper function implementations */

/**
 * setup_python_config - Sets up the Python configuration
 *
 * The function imports the Config class from src.utils.config,
 * creates an instance of it, and returns the instance.
 *
 * Return: PyObject pointer to the Config instance, or NULL on failure
 */
static PyObject *setup_python_config(void)
{
	PyObject *pConfigModule, *pConfigClass, *pConfigInstance;

	pConfigModule = PyImport_ImportModule("src.utils.config");
	if (!pConfigModule)
	{
		PyErr_Print();
		fprintf(stderr, "Failed to import src.utils.config\n");
		return (NULL);
	}

	pConfigClass = PyObject_GetAttrString(pConfigModule, "Config");
	if (!pConfigClass)
	{
		PyErr_Print();
		Py_DECREF(pConfigModule);
		return (NULL);
	}

	pConfigInstance = PyObject_CallObject(pConfigClass, NULL);
	if (!pConfigInstance)
	{
		PyErr_Print();
		Py_DECREF(pConfigClass);
		Py_DECREF(pConfigModule);
		return (NULL);
	}

	Py_DECREF(pConfigClass);
	Py_DECREF(pConfigModule);
	return (pConfigInstance);
}

/**
 * setup_task_processor - Sets up the TaskProcessor
 * @pConfigInstance: PyObject pointer to the Config instance
 *
 * The function imports the TaskProcessor class from src.task_processor,
 * creates an instance of it with the given Config instance,
 * and returns the process_tasks method of the TaskProcessor instance.
 *
 * Return: PyObject pointer to the process_tasks method, or NULL on failure
 */
static PyObject *setup_task_processor(PyObject *pConfigInstance)
{
	PyObject *pModule, *pClass, *pArgs, *pInstance, *pFunc;

	pModule = PyImport_ImportModule("src.task_processor");
	if (!pModule)
	{
		PyErr_Print();
		return (NULL);
	}

	pClass = PyObject_GetAttrString(pModule, "TaskProcessor");
	if (!pClass)
	{
		PyErr_Print();
		Py_DECREF(pModule);
		return (NULL);
	}

	pArgs = PyTuple_Pack(1, pConfigInstance);
	pInstance = PyObject_CallObject(pClass, pArgs);
	Py_DECREF(pArgs);

	if (!pInstance)
	{
		PyErr_Print();
		Py_DECREF(pClass);
		Py_DECREF(pModule);
		return (NULL);
	}

	pFunc = PyObject_GetAttrString(pInstance, "process_tasks");
	if (!pFunc || !PyCallable_Check(pFunc))
	{
		PyErr_Print();
		Py_DECREF(pInstance);
		Py_DECREF(pClass);
		Py_DECREF(pModule);
		return (NULL);
	}

	Py_DECREF(pInstance);
	Py_DECREF(pClass);
	Py_DECREF(pModule);
	return (pFunc);
}

/**
 * setup_asyncio - Sets up the asyncio event loop
 *
 * The function imports the asyncio module and retrieves the event loop.
 *
 * Return: PyObject pointer to the event loop, or NULL on failure
 */
static PyObject *setup_asyncio(void)
{
	PyObject *asyncio_module, *get_event_loop, *loop;

	asyncio_module = PyImport_ImportModule("asyncio");
	if (!asyncio_module)
	{
		PyErr_Print();
		return (NULL);
	}

	get_event_loop = PyObject_GetAttrString(asyncio_module, "get_event_loop");
	if (!get_event_loop)
	{
		PyErr_Print();
		Py_DECREF(asyncio_module);
		return (NULL);
	}

	loop = PyObject_CallObject(get_event_loop, NULL);
	if (!loop)
	{
		PyErr_Print();
		Py_DECREF(get_event_loop);
		Py_DECREF(asyncio_module);
		return (NULL);
	}

	Py_DECREF(get_event_loop);
	Py_DECREF(asyncio_module);
	return (loop);
}

/**
 * parse_task_json - Parses the task JSON string
 * @task: The task JSON string to parse
 * @task_content: Pointer to store the parsed task content
 * @ip_address: Pointer to store the parsed IP address
 * @task_id: Pointer to store the parsed task ID
 * @client_ip: Pointer to store the parsed client IP
 * @task_type: Pointer to store the parsed task type
 *
 * The function parses the given task JSON string and extracts
 * the relevant fields.
 *
 * Return: 1 on success, 0 on failure
 */
static int parse_task_json(const char *task, json_object **task_content,
			   json_object **ip_address, json_object **task_id,
			   json_object **client_ip, json_object **task_type)
{
	json_object *task_obj = json_tokener_parse(task);

	if (!json_object_object_get_ex(task_obj, "task", task_content) ||
	    !json_object_object_get_ex(*task_content, "task", ip_address) ||
	    !json_object_object_get_ex(*task_content, "task_id", task_id) ||
	    !json_object_object_get_ex(*task_content, "client_ip", client_ip) ||
	    !json_object_object_get_ex(*task_content, "task_type", task_type))
	{
		fprintf(stderr, "Failed to parse task JSON\n");
		json_object_put(task_obj);
		return (0);
	}

	return (1);
}

/**
 * create_python_task_dict - Creates a Python dictionary from task data
 * @ip_address: JSON object containing the IP address
 * @task_id: JSON object containing the task ID
 * @client_ip: JSON object containing the client IP
 * @task_type: JSON object containing the task type
 *
 * The function creates a Python dictionary containing the task data
 * extracted from the JSON objects.
 *
 * Return: PyObject pointer to the created dictionary, or NULL on failure
 */
static PyObject *create_python_task_dict(json_object *ip_address,
					 json_object *task_id,
					 json_object *client_ip,
					 json_object *task_type)
{
	PyObject *pTaskDict = PyDict_New();

	if (!pTaskDict)
	{
		fprintf(stderr, "Failed to create Python dictionary\n");
		PyErr_Print();
		return (NULL);
	}

	PyDict_SetItemString(pTaskDict, "task",
			     PyUnicode_FromString(json_object_get_string(ip_address)));
	PyDict_SetItemString(pTaskDict, "task_id",
			     PyUnicode_FromString(json_object_get_string(task_id)));
	PyDict_SetItemString(pTaskDict, "client_ip",
			     PyUnicode_FromString(json_object_get_string(client_ip)));
	PyDict_SetItemString(pTaskDict, "task_type",
			     PyUnicode_FromString(json_object_get_string(task_type)));

	return (pTaskDict);
}
