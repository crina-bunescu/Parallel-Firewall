// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

static so_log_entry_t **heap;
static int heap_size;
static int heap_cap;
static int next_to_write;
static int total_packets;
static int out_fd_global;

static pthread_mutex_t heap_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t heap_cond = PTHREAD_COND_INITIALIZER;

static void heap_swap(int a, int b)
{
	so_log_entry_t *tmp = heap[a];

	heap[a] = heap[b];
	heap[b] = tmp;
}

static void heap_push(so_log_entry_t *e)
{
	int i = heap_size++;

	heap[i] = e;

	while (i > 0) {
		int parent = (i - 1) / 2;

		if (heap[parent]->index <= heap[i]->index)
			break;
		heap_swap(i, parent);
		i = parent;
	}
}

static so_log_entry_t *heap_pop(void)
{
	if (heap_size == 0)
		return NULL;

	so_log_entry_t *root = heap[0];

	heap_size--;
	heap[0] = heap[heap_size];

	int i = 0;

	while (1) {
		int left = 2*i + 1, right = 2*i + 2, smallest = i;

		if (left < heap_size && heap[left]->index < heap[smallest]->index)
			smallest = left;

		if (right < heap_size && heap[right]->index < heap[smallest]->index)
			smallest = right;

		if (smallest == i)
			break;

		heap_swap(i, smallest);
		i = smallest;
	}

	return root;
}

void submit_log(unsigned long index, char *line)
{
	so_log_entry_t *e = malloc(sizeof(*e));

	e->index = index;
	e->line = line;

	pthread_mutex_lock(&heap_mutex);
	heap_push(e);
	pthread_cond_signal(&heap_cond);
	pthread_mutex_unlock(&heap_mutex);
}

void init_log_system(int total, int out_fd)
{
	total_packets = total;

	heap_cap = total_packets;
	heap_size = 0;

	heap = malloc(sizeof(so_log_entry_t *) * heap_cap);
	DIE(heap == NULL, "heap malloc");

	next_to_write = 0;
	out_fd_global = out_fd;

	pthread_mutex_init(&heap_mutex, NULL);
	pthread_cond_init(&heap_cond, NULL);
}

void close_log_system(void)
{
	for (int i = 0; i < heap_size; i++) {
		free(heap[i]->line);
		free(heap[i]);
	}

	free(heap);

	pthread_mutex_destroy(&heap_mutex);
	pthread_cond_destroy(&heap_cond);
}

void *writer_thread(void *arg)
{
	(void)arg;
	while (next_to_write < total_packets) {
		pthread_mutex_lock(&heap_mutex);

		while (heap_size == 0 || heap[0]->index != next_to_write)
			pthread_cond_wait(&heap_cond, &heap_mutex);

		so_log_entry_t *e = heap_pop();

		pthread_mutex_unlock(&heap_mutex);

		write(out_fd_global, e->line, strlen(e->line));
		free(e->line);
		free(e);

		next_to_write++;
	}

	return NULL;
}

void consumer_thread(so_consumer_ctx_t *ctx)
{
	/* TODO: implement consumer thread */
	so_ring_buffer_t *rb = ctx->producer_rb;

	char buffer[sizeof(so_indexed_packet_t)];
	char out_buf[256];

	while (1) {
		ssize_t ret = ring_buffer_dequeue(rb, buffer, sizeof(so_indexed_packet_t));

		if (ret == 0)
			break;

		so_indexed_packet_t *ipkt = (so_indexed_packet_t *)buffer;
		unsigned long index = ipkt->index;

		struct so_packet_t *pkt = (struct so_packet_t *)ipkt->data;

		int action = process_packet(pkt);
		unsigned long hash = packet_hash(pkt);
		unsigned long timestamp = pkt->hdr.timestamp;

		int len = snprintf(out_buf, 256, "%s %016lx %lu\n",
			RES_TO_STR(action), hash, timestamp);

		submit_log(index, strndup(out_buf, len));
	}
}

static void *consumer_start_routine(void *arg)
{
	so_consumer_ctx_t *ctx = (so_consumer_ctx_t *)arg;

	consumer_thread(ctx);
	free(ctx);

	return NULL;
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb)
{
	for (int i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/
		so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));

		ctx->producer_rb = rb;
		pthread_create(&tids[i], NULL, consumer_start_routine, ctx);
	}

	return num_consumers;
}
