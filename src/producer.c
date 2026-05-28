// SPDX-License-Identifier: BSD-3-Clause

#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"
#include "producer.h"
#include "consumer.h"

void publish_data(so_ring_buffer_t *rb, const char *filename)
{
	ssize_t sz;
	int fd;

	int idx = 0;
	so_indexed_packet_t idx_pkt;

	fd = open(filename, O_RDONLY);
	DIE(fd < 0, "open");

	while ((sz = read(fd, idx_pkt.data, PKT_SZ)) != 0) {
		DIE(sz != PKT_SZ, "packet truncated");

		idx_pkt.index = idx;
		idx++;

		/* enequeue packet into ring buffer */
		ring_buffer_enqueue(rb, &idx_pkt, sizeof(idx_pkt));
	}

	ring_buffer_stop(rb);
}
