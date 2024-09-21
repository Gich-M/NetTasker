#include "worker.h"

/**
 * WriteMemoryCallback - Callback function to handle memory writing
 * @contents: Pointer to the received data
 * @size: Size of each data element
 * @nmemb: Number of data elements
 * @userp: Pointer to the MemoryStruct
 *
 * Return: Total size of the data written
 */
static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb,
				  void *userp)
{
	size_t realsize = size * nmemb;
	MemoryStruct *mem = (MemoryStruct *)userp;

	char *ptr = realloc(mem->memory, mem->size + realsize + 1);

	if (!ptr)
	{
		fprintf(stderr, "Memory allocation failed\n");
		free(mem->memory);
		mem->memory = NULL;
		mem->size = 0;
		return (0);
	}

	mem->memory = ptr;
	memcpy(&(mem->memory[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->memory[mem->size] = 0;

	return (realsize);
}

/**
 * request_task - Sends a request to the server for a new task
 * @curl: CURL handle
 * @host: Server hostname
 * @port: Server port
 * @worker_id: ID of the worker
 * @chunk: Pointer to MemoryStruct to store the response
 * @task_type: Type of task to request
 *
 * Return: 1 on success, 0 on failure
 */
int request_task(CURL *curl, const char *host, int port, const char *worker_id,
		 MemoryStruct *chunk, const char *task_type)
{
	char url[MAX_URL_LENGTH];
	json_object *request;
	const char *request_str;
	CURLcode res;

	snprintf(url, MAX_URL_LENGTH, "http://%s:%d/process_task", host, port);
	printf("Requesting task from URL: %s\n", url);

	request = json_object_new_object();
	json_object_object_add(request, "worker_id",
			       json_object_new_string(worker_id));
	json_object_object_add(request, "task_type",
			       json_object_new_string(task_type));
	request_str = json_object_to_json_string(request);
	printf("Request payload: %s\n", request_str);

	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_str);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)chunk);

	printf("Sending request...\n");
	res = curl_easy_perform(curl);
	json_object_put(request);

	if (res != CURLE_OK)
	{
		handle_curl_error(curl, res);
		return (0);
	}

	printf("Request successful. Response size: %zu bytes\n", chunk->size);
	printf("Response content: %s\n", chunk->memory);
	return (1);
}

/**
 * handle_curl_error - Handles and logs CURL errors
 * @curl: CURL handle
 * @res: CURL result code
 */
static void handle_curl_error(CURL *curl, CURLcode res)
{
	long response_code;
	char *effective_url = NULL;

	fprintf(stderr, "Failed to request task: %s\n", curl_easy_strerror(res));
	fprintf(stderr, "CURL error code: %d\n", res);

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
	fprintf(stderr, "HTTP response code: %ld\n", response_code);

	curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &effective_url);
	fprintf(stderr, "Effective URL: %s\n",
		effective_url ? effective_url : "N/A");
}

/**
 * submit_result - Submit the task result back to the server
 * @curl: CURL handle
 * @host: Server hostname
 * @port: Server port
 * @worker_id: ID of the worker
 * @result_str: String containing the task result
 * @task_type: Type of task that was processed
 *
 * Return: 1 on success, 0 on failure
 */
int submit_result(CURL *curl, const char *host, int port,
	const char *worker_id, const char *result_str, const char *task_type)
{
	char url[MAX_URL_LENGTH];
	json_object *result;
	const char *result_json;
	CURLcode res;

	snprintf(url, MAX_URL_LENGTH, "http://%s:%d/submit_result", host, port);
	result = json_object_new_object();
	json_object_object_add(result, "worker_id",
			       json_object_new_string(worker_id));
	json_object_object_add(result, "result",
			       json_object_new_string(result_str));
	json_object_object_add(result, "task_type",
			       json_object_new_string(task_type));
	result_json = json_object_to_json_string(result);

	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, result_json);
	res = curl_easy_perform(curl);
	json_object_put(result);

	if (res != CURLE_OK)
	{
		fprintf(stderr, "Failed to submit result: %s\n",
			curl_easy_strerror(res));
		return (0);
	}

	return (1);
}
