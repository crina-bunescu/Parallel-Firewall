// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	ring->data = malloc(cap);

	if (ring->data == NULL)
		return -1;

	ring->read_pos = 0;
	ring->write_pos = 0;
	ring->len = 0;
	ring->cap = cap;

	pthread_mutex_init(&ring->mutex, NULL);
	pthread_cond_init(&ring->not_empty, NULL);
	pthread_cond_init(&ring->not_full, NULL);

	ring->end = 0;

	return 1;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */
	pthread_mutex_lock(&ring->mutex);

	while (ring->len + size > ring->cap)
		pthread_cond_wait(&ring->not_full, &ring->mutex);

	size_t space_available = ring->cap - ring->write_pos;

	if (size <= space_available) {
		memcpy(ring->data + ring->write_pos, data, size);
	} else {
		memcpy(ring->data + ring->write_pos, data, space_available);
		char *rest_data = data + space_available;

		memcpy(ring->data, rest_data, size - space_available);
	}

	ring->write_pos = (ring->write_pos + size) % ring->cap;
	ring->len = ring->len + size;

	pthread_cond_signal(&ring->not_empty);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */
	pthread_mutex_lock(&ring->mutex);

	while (ring->len < size && !ring->end)
		pthread_cond_wait(&ring->not_empty, &ring->mutex);

	if (ring->len < size && ring->end) {
		pthread_mutex_unlock(&ring->mutex);
		return 0;
	}

	size_t space_available = ring->cap - ring->read_pos;

	if (size <= space_available) {
		memcpy(data, ring->data + ring->read_pos, size);
	} else {
		memcpy(data, ring->data + ring->read_pos, space_available);
		char *rest_data = data + space_available;

		memcpy(rest_data, ring->data, size - space_available);
	}

	ring->read_pos = (ring->read_pos + size) % ring->cap;
	ring->len = ring->len - size;

	pthread_cond_signal(&ring->not_full);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	if (!ring)
		return;

	free(ring->data);
	ring->data = NULL;

	pthread_mutex_destroy(&ring->mutex);
	pthread_cond_destroy(&ring->not_empty);
	pthread_cond_destroy(&ring->not_full);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	pthread_mutex_lock(&ring->mutex);

	ring->end = 1;

	pthread_cond_broadcast(&ring->not_empty);
	pthread_mutex_unlock(&ring->mutex);
}
