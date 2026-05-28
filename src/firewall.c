// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "ring_buffer.h"
#include "consumer.h"
#include "producer.h"
#include "log/log.h"
#include "packet.h"
#include "utils.h"

#define SO_RING_SZ (sizeof(so_indexed_packet_t) * 1000)

pthread_mutex_t MUTEX_LOG;

static int count_packets(const char *filename)
{
	struct stat st;
	int rc = stat(filename, &st);

	DIE(rc < 0, "stat");

	return st.st_size / PKT_SZ;
}

void log_lock(bool lock, void *udata)
{
	pthread_mutex_t *LOCK = (pthread_mutex_t *) udata;

	if (lock)
		pthread_mutex_lock(LOCK);
	else
		pthread_mutex_unlock(LOCK);
}

void __attribute__((constructor)) init()
{
	pthread_mutex_init(&MUTEX_LOG, NULL);
	log_set_lock(log_lock, &MUTEX_LOG);
}

void __attribute__((destructor)) dest()
{
	pthread_mutex_destroy(&MUTEX_LOG);
}

int main(int argc, char **argv)
{
	so_ring_buffer_t ring_buffer;
	int num_consumers, threads, rc;
	pthread_t *thread_ids = NULL;
	pthread_t writer_tid;
	int out_fd;
	int total_packets;

	if (argc < 4) {
		fprintf(stderr, "Usage %s <input-file> <output-file> <num-consumers:1-32>\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	rc = ring_buffer_init(&ring_buffer, SO_RING_SZ);
	DIE(rc < 0, "ring_buffer_init");

	total_packets = count_packets(argv[1]);

	num_consumers = strtol(argv[3], NULL, 10);

	if (num_consumers <= 0 || num_consumers > 32) {
		fprintf(stderr, "num-consumers [%d] must be in the interval [1-32]\n", num_consumers);
		exit(EXIT_FAILURE);
	}

	out_fd = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0666);
	DIE(out_fd < 0, "open");

	init_log_system(total_packets, out_fd);

	thread_ids = calloc(num_consumers, sizeof(pthread_t));
	DIE(thread_ids == NULL, "calloc pthread_t");

	/* create consumer threads */
	threads = create_consumers(thread_ids, num_consumers, &ring_buffer);

	pthread_create(&writer_tid, NULL, writer_thread, NULL);
	publish_data(&ring_buffer, argv[1]);

	/* TODO: wait for child threads to finish execution*/
	for (int i = 0; i < threads; i++)
		pthread_join(thread_ids[i], NULL);

	pthread_join(writer_tid, NULL);

	ring_buffer_destroy(&ring_buffer);
	close_log_system();

	free(thread_ids);
	close(out_fd);

	return 0;
}

