/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include "ring_buffer.h"
#include "packet.h"

typedef struct so_consumer_ctx_t {
	struct so_ring_buffer_t *producer_rb;

    /* TODO: add synchronization primitives for timestamp ordering */
	int out_fd;
	pthread_mutex_t *write_mutex;
} so_consumer_ctx_t;

typedef struct so_indexed_packet {
	int index;
	char data[PKT_SZ];
} so_indexed_packet_t;

typedef struct so_log_entry {
	int index;
	char *line;
} so_log_entry_t;

int create_consumers(pthread_t *tids,
					int num_consumers,
					so_ring_buffer_t *rb);

void submit_log(unsigned long index, char *line);
void init_log_system(int total_packets, int out_fd);
void close_log_system(void);

void *writer_thread(void *arg);
#endif /* __SO_CONSUMER_H__ */
