#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sched.h>
#include <signal.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <semaphore.h>
#include "common.h"

#define BACKLOG_COUNT 100
#define USAGE_STRING "Missing parameter. Exiting.\n" \
                     "Usage: %s -q <queue size> " \
                     "-w <workers: 1> " \
                     "-p <policy: FIFO> " \
                     "<port_number>\n"
#define STACK_SIZE (4096)
#define MAX_IMAGES 16
#define MAX_WAITING_REQUESTS 1500

sem_t *printf_mutex;
sem_t **image_semaphores;
sem_t array_mutex;
sem_t conn_socket_sem;

#define sync_printf(...) \
    do { \
        sem_wait(printf_mutex); \
        printf(__VA_ARGS__); \
        sem_post(printf_mutex); \
    } while (0)

sem_t *queue_mutex;
sem_t *queue_notify;

struct image **images = NULL;
uint64_t image_count = 0;

struct request_meta {
    struct request request;
    struct timespec receipt_timestamp;
    struct timespec start_timestamp;
    struct timespec completion_timestamp;
};

enum queue_policy {
    QUEUE_FIFO,
    QUEUE_SJN
};

struct queue {
    size_t wr_pos;
    size_t rd_pos;
    size_t max_size;
    size_t available;
	size_t count;
    enum queue_policy policy;
    struct request_meta *requests;
};

struct connection_params {
    size_t queue_size;
    size_t workers;
    enum queue_policy queue_policy;
};

struct worker_params {
    int conn_socket;
    int worker_done;
    struct queue *the_queue;
    int worker_id;
};

enum worker_command {
    WORKERS_START,
    WORKERS_STOP
};

struct waiting_requests {
    uint64_t requests[MAX_WAITING_REQUESTS];
    int count;
};

struct waiting_requests waiting[MAX_IMAGES];

void add_waiting_request(uint64_t img_id, int req_id) {
    waiting[img_id].requests[waiting[img_id].count++] = req_id;
}

void remove_completed_request(uint64_t img_id, uint64_t req_id) {
    for (int i = 0; i < waiting[img_id].count; ++i) {
        if (waiting[img_id].requests[i] == req_id) {
            for (int j = i; j < waiting[img_id].count - 1; ++j) {
                waiting[img_id].requests[j] = waiting[img_id].requests[j + 1];
            }
            waiting[img_id].count--;
            break;
        }
    }
}

void initialize_image_semaphores() {
    image_semaphores = (sem_t **)malloc(MAX_IMAGES * sizeof(sem_t *));
    if (image_semaphores == NULL) {
        perror("Failed to allocate memory for image semaphores");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < MAX_IMAGES; ++i) {
        image_semaphores[i] = (sem_t *)malloc(sizeof(sem_t));
        if (sem_init(image_semaphores[i], 0, 1) != 0) {
            perror("Failed to initialize image semaphore");
            exit(EXIT_FAILURE);
        }
    }

    if (sem_init(&array_mutex, 0, 1) != 0) {
        perror("Failed to initialize image registration mutex");
        exit(EXIT_FAILURE);
    }
}

void destroy_image_semaphores() {
    for (int i = 0; i < MAX_IMAGES; ++i) {
        sem_destroy(image_semaphores[i]);
        free(image_semaphores[i]);
    }
    free(image_semaphores);
    image_semaphores = NULL;

    sem_destroy(&array_mutex);
}

void initialize_array_mutex() {
    if (sem_init(&array_mutex, 0, 1) != 0) {
        perror("Failed to initialize array mutex");
        exit(EXIT_FAILURE);
    }
}

void destroy_array_mutex() {
    sem_destroy(&array_mutex);
}

void initialize_connection_socket_semaphore() {
    if (sem_init(&conn_socket_sem, 0, 1) != 0) {
        perror("Failed to initialize connection socket semaphore");
        exit(EXIT_FAILURE);
    }
}

void destroy_connection_socket_semaphore() {
    sem_destroy(&conn_socket_sem);
}

void lock_image_semaphore(int image_id) {
    sem_wait(image_semaphores[image_id]);
}

void unlock_image_semaphore(int image_id) {
    sem_post(image_semaphores[image_id]);
}

void lock_array_mutex() {
    sem_wait(&array_mutex);
}

void unlock_array_mutex() {
    sem_post(&array_mutex);
}

void lock_socket_semaphore() {
    sem_wait(&conn_socket_sem);
}

void unlock_socket_semaphore() {
    sem_post(&conn_socket_sem);
}

void queue_init(struct queue * the_queue, size_t queue_size, enum queue_policy policy) {
    the_queue->rd_pos = 0;
    the_queue->wr_pos = 0;
    the_queue->max_size = queue_size;
    the_queue->requests = (struct request_meta *)malloc(sizeof(struct request_meta) * the_queue->max_size);
    the_queue->available = queue_size;
    the_queue->policy = policy;
}

int add_to_queue(struct request_meta to_add, struct queue *the_queue, enum queue_policy policy)
{
    int retval = 0;
    sem_wait(queue_mutex);

    if (the_queue->count >= the_queue->max_size) {
        retval = -1;
    } else {
        if (policy == QUEUE_FIFO) {
            the_queue->requests[the_queue->wr_pos] = to_add;
            the_queue->wr_pos = (the_queue->wr_pos + 1) % the_queue->max_size;
        } else if (policy == QUEUE_SJN) {
            int insert_index = 0;
            for (int i = 0; i < the_queue->count; i++) {
                if (TSPEC_TO_DOUBLE(to_add.request.req_length) < TSPEC_TO_DOUBLE(the_queue->requests[(the_queue->rd_pos + i) % the_queue->max_size].request.req_length)) {
                    insert_index = i;
                    break;
                }
            }
            for (int i = the_queue->count; i > insert_index; i--) {
                the_queue->requests[(the_queue->rd_pos + i) % the_queue->max_size] = the_queue->requests[(the_queue->rd_pos + i - 1) % the_queue->max_size];
            }
            the_queue->requests[(the_queue->rd_pos + insert_index) % the_queue->max_size] = to_add;
        }
        the_queue->count++;
        sem_post(queue_notify);
    }
    sem_post(queue_mutex);
    return retval;
}

struct request_meta get_from_queue(struct queue * the_queue) {
    struct request_meta retval;

    sem_wait(queue_notify);
    sem_wait(queue_mutex);

    retval = the_queue->requests[the_queue->rd_pos];
    the_queue->rd_pos = (the_queue->rd_pos + 1) % the_queue->max_size;
    the_queue->available++;

    sem_post(queue_mutex);
    return retval;
}

void dump_queue_status(struct queue * the_queue) {
    size_t i, j;

    sem_wait(queue_mutex);

    sem_wait(printf_mutex);
    printf("Q:[");

    for (i = the_queue->rd_pos, j = 0; j < the_queue->max_size - the_queue->available;
         i = (i + 1) % the_queue->max_size, ++j)
    {
        printf("R%ld%s", the_queue->requests[i].request.req_id,
               ((j + 1 != the_queue->max_size - the_queue->available) ? "," : ""));
    }

    printf("]\n");
    sem_post(printf_mutex);

    sem_post(queue_mutex);
}

void register_new_image(int conn_socket, struct request *req) {
    lock_socket_semaphore();
    lock_array_mutex();

    image_count++;
    images = realloc(images, image_count * sizeof(struct image *));

    if (images == NULL) {
        unlock_array_mutex();
        perror("Failed to reallocate memory for images");
        exit(EXIT_FAILURE);
    }

    struct image *new_img = recvImage(conn_socket);
    images[image_count - 1] = new_img;

    unlock_array_mutex();

    struct response resp;
    resp.req_id = req->req_id;
    resp.img_id = image_count - 1;
    resp.ack = RESP_COMPLETED;
    send(conn_socket, &resp, sizeof(struct response), 0);

    unlock_socket_semaphore();
}

int worker_main(void *arg) {
	struct timespec now;
	struct worker_params *params = (struct worker_params *)arg;
	clock_gettime(CLOCK_MONOTONIC, &now);
	sync_printf("[#WORKER#] %lf Worker Thread Alive!\n", TSPEC_TO_DOUBLE(now));
	while (!params->worker_done) {
		struct request_meta req;
		struct response resp;
		struct image *img = NULL;
		uint64_t img_id;
		req = get_from_queue(params->the_queue);
		if (params->worker_done)
			break;
		img_id = req.request.img_id;
		lock_image_semaphore(img_id);
		int acquired = 0;
        while (!acquired) {
            if (waiting[img_id].requests[0] == req.request.req_id) {
                acquired = 1;
            }
            else {
				unlock_image_semaphore(img_id);
				lock_image_semaphore(img_id);
			}
        }
        remove_completed_request(img_id, req.request.req_id);
		clock_gettime(CLOCK_MONOTONIC, &req.start_timestamp);
		img = images[img_id];
		assert(img != NULL); 
		switch (req.request.img_op) {
		case IMG_ROT90CLKW:
			img = rotate90Clockwise(img, NULL);
			break;
		case IMG_BLUR:
			img = blurImage(img);
			break;
		case IMG_SHARPEN:
			img = sharpenImage(img);
			break;
		case IMG_VERTEDGES:
			img = detectVerticalEdges(img);
			break;
		case IMG_HORIZEDGES:
			img = detectHorizontalEdges(img);
			break;
		}
		lock_array_mutex();
		if (req.request.img_op != IMG_RETRIEVE) {
			if (req.request.overwrite) {
				deleteImage(images[img_id]);
				images[img_id] = img;
			} else {
				img_id = image_count++;
				images = realloc(images, image_count * sizeof(struct image *));
				images[img_id] = img;
			}
		}
		unlock_array_mutex();
		clock_gettime(CLOCK_MONOTONIC, &req.completion_timestamp);
		lock_socket_semaphore();
		resp.req_id = req.request.req_id;
		resp.ack = RESP_COMPLETED;
		resp.img_id = img_id;
		send(params->conn_socket, &resp, sizeof(struct response), 0);
		if (req.request.img_op == IMG_RETRIEVE) {
			uint8_t err = sendImage(img, params->conn_socket);
			if(err) {
				ERROR_INFO();
				perror("Unable to send image payload to client.");
			}
		}
		unlock_socket_semaphore();
		unlock_image_semaphore(img_id);
		sync_printf("T%d R%ld:%lf,%s,%d,%ld,%ld,%lf,%lf,%lf\n",
		       params->worker_id, req.request.req_id,
		       TSPEC_TO_DOUBLE(req.request.req_timestamp),
		       OPCODE_TO_STRING(req.request.img_op),
		       req.request.overwrite, req.request.img_id, img_id,
		       TSPEC_TO_DOUBLE(req.receipt_timestamp),
		       TSPEC_TO_DOUBLE(req.start_timestamp),
		       TSPEC_TO_DOUBLE(req.completion_timestamp));
		dump_queue_status(params->the_queue);
	}
	return EXIT_SUCCESS;
}

int control_workers(enum worker_command cmd, size_t worker_count,
		    struct worker_params * common_params)
{
	static char ** worker_stacks = NULL;
	static struct worker_params ** worker_params = NULL;
	static int * worker_ids = NULL;
	if (cmd == WORKERS_START) {
		size_t i;
		worker_stacks = (char **)malloc(worker_count * sizeof(char *));
		worker_params = (struct worker_params **)
			malloc(worker_count * sizeof(struct worker_params *));
		worker_ids = (int *)malloc(worker_count * sizeof(int));
		if (!worker_stacks || !worker_params) {
			ERROR_INFO();
			perror("Unable to allocate descriptor arrays for threads.");
			return EXIT_FAILURE;
		}
		for (i = 0; i < worker_count; ++i) {
			worker_ids[i] = -1;
			worker_stacks[i] = malloc(STACK_SIZE);
			worker_params[i] = (struct worker_params *)
				malloc(sizeof(struct worker_params));
			if (!worker_stacks[i] || !worker_params[i]) {
				ERROR_INFO();
				perror("Unable to allocate memory for thread.");
				return EXIT_FAILURE;
			}
			worker_params[i]->conn_socket = common_params->conn_socket;
			worker_params[i]->the_queue = common_params->the_queue;
			worker_params[i]->worker_done = 0;
			worker_params[i]->worker_id = i;
		}
		for (i = 0; i < worker_count; ++i) {
			worker_ids[i] = clone(worker_main, worker_stacks[i] + STACK_SIZE,
					      CLONE_THREAD | CLONE_VM | CLONE_SIGHAND |
					      CLONE_FS | CLONE_FILES | CLONE_SYSVSEM,
					      worker_params[i]);
			if (worker_ids[i] < 0) {
				ERROR_INFO();
				perror("Unable to start thread.");
				return EXIT_FAILURE;
			} else {
				sync_printf("INFO: Worker thread %ld (TID = %d) started!\n",
				       i, worker_ids[i]);
			}
		}
	}
	else if (cmd == WORKERS_STOP) {
		size_t i;
		if (!worker_stacks || !worker_params || !worker_ids) {
			return EXIT_FAILURE;
		}
		for (i = 0; i < worker_count; ++i) {
			if (worker_ids[i] < 0) {
				continue;
			}
			worker_params[i]->worker_done = 1;
		}
		for (i = 0; i < worker_count; ++i) {
			if (worker_ids[i] < 0) {
				continue;
			}
			sem_post(queue_notify);
			waitpid(-1, NULL, __WCLONE);
			sync_printf("INFO: Worker thread exited.\n");
		}
		for (i = 0; i < worker_count; ++i) {
			free(worker_stacks[i]);
			free(worker_params[i]);
		}
		free(worker_stacks);
		worker_stacks = NULL;
		free(worker_params);
		worker_params = NULL;
		free(worker_ids);
		worker_ids = NULL;
	}
	else {
		ERROR_INFO();
		perror("Invalid thread control command.");
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}

void handle_connection(int conn_socket, struct connection_params conn_params)
{
	struct request_meta * req;
	struct queue * the_queue;
	size_t in_bytes;
	struct worker_params common_worker_params;
	int res;
	the_queue = (struct queue *)malloc(sizeof(struct queue));
	queue_init(the_queue, conn_params.queue_size, conn_params.queue_policy);
	common_worker_params.conn_socket = conn_socket;
	common_worker_params.the_queue = the_queue;
	res = control_workers(WORKERS_START, conn_params.workers, &common_worker_params);
	if (res != EXIT_SUCCESS) {
		free(the_queue);
		control_workers(WORKERS_STOP, conn_params.workers, NULL);
		return;
	}
	req = (struct request_meta *)malloc(sizeof(struct request_meta));
	do {
		in_bytes = recv(conn_socket, &req->request, sizeof(struct request), 0);
		clock_gettime(CLOCK_MONOTONIC, &req->receipt_timestamp);
		if (in_bytes > 0) {
			if(req->request.img_op == IMG_REGISTER) {
				clock_gettime(CLOCK_MONOTONIC, &req->start_timestamp);
				register_new_image(conn_socket, &req->request);
				clock_gettime(CLOCK_MONOTONIC, &req->completion_timestamp);
				sync_printf("T%ld R%ld:%lf,%s,%d,%ld,%ld,%lf,%lf,%lf\n",
				       conn_params.workers, req->request.req_id,
				       TSPEC_TO_DOUBLE(req.request.req_timestamp),
				       OPCODE_TO_STRING(req.request.img_op),
				       req.request.overwrite, req.request.img_id,
				       image_count - 1,
				       TSPEC_TO_DOUBLE(req.receipt_timestamp),
				       TSPEC_TO_DOUBLE(req.start_timestamp),
				       TSPEC_TO_DOUBLE(req.completion_timestamp));
				dump_queue_status(the_queue);
				continue;
			}
			add_waiting_request(req->request.img_id, req->request.req_id);
			res = add_to_queue(*req, the_queue);
			if (res) {
				struct response resp;
				resp.req_id = req->request.req_id;
				resp.ack = RESP_REJECTED;
				send(conn_socket, &resp, sizeof(struct response), 0);
				sync_printf("X%ld:%lf,%lf,%lf\n", req->request.req_id,
				       TSPEC_TO_DOUBLE(req.request.req_timestamp),
				       TSPEC_TO_DOUBLE(req.request.req_length),
				       TSPEC_TO_DOUBLE(req.receipt_timestamp)
					);
			}
		}
	} while (in_bytes > 0);
	free(req);
	shutdown(conn_socket, SHUT_RDWR);
	close(conn_socket);
	printf("INFO: Client disconnected.\n");
}

int main(int argc, char **argv) {
    int sockfd, retval, accepted, optval, opt;
    in_port_t socket_port;
    struct sockaddr_in addr, client;
    struct in_addr any_address;
    socklen_t client_len;
    struct connection_params conn_params;
    conn_params.queue_size = 0;
    conn_params.queue_policy = QUEUE_FIFO;
    conn_params.workers = 1;

    while ((opt = getopt(argc, argv, "q:w:p:")) != -1) {
        switch (opt) {
            case 'q':
                conn_params.queue_size = strtol(optarg, NULL, 10);
                break;
            case 'w':
                conn_params.workers = strtol(optarg, NULL, 10);
                if (conn_params.workers > 10) {
                    return EXIT_FAILURE;
                }
                break;
            case 'p':
                if (strcmp(optarg, "FIFO") == 0) {
                    conn_params.queue_policy = 0;
                } else if (strcmp(optarg, "SJN") == 0) {
                    conn_params.queue_policy = 1;
                } else {
                    fprintf(stderr, "Error: Invalid policy. Use 'FIFO' or 'SJN'.\n");
                    fprintf(stderr, USAGE_STRING, argv[0]);
                    exit(EXIT_FAILURE);
                }
                break;
            default:
                fprintf(stderr, USAGE_STRING, argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (!conn_params.queue_size) {
        fprintf(stderr, USAGE_STRING, argv[0]);
        return EXIT_FAILURE;
    }

    if (optind < argc) {
        socket_port = strtol(argv[optind], NULL, 10);
    } else {
        fprintf(stderr, USAGE_STRING, argv[0]);
        return EXIT_FAILURE;
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        return EXIT_FAILURE;
    }

    optval = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&optval, sizeof(optval));

    any_address.s_addr = htonl(INADDR_ANY);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(socket_port);
    addr.sin_addr = any_address;

    retval = bind(sockfd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));

    if (retval < 0) {
        return EXIT_FAILURE;
    }

    retval = listen(sockfd, BACKLOG_COUNT);

    if (retval < 0) {
        return EXIT_FAILURE;
    }

    printf("INFO: Waiting for incoming connection...\n");
    client_len = sizeof(struct sockaddr_in);
    accepted = accept(sockfd, (struct sockaddr *)&client, &client_len);

    if (accepted == -1) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
} 