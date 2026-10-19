// SPDX-License-Identifier: BSD-3-Clause

/*
 * DISCLAIMER:
 * Implementarea a mai multe functii este luata copy-paste din
 * epoll_echo_server.c, fisier valabil in directorul src/samples
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

#define BACKLOG SOMAXCONN

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	if (len >= BUFSIZ)
		len = BUFSIZ - 1;
	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	conn->send_len = snprintf(conn->send_buffer, BUFSIZ,
				  "HTTP/1.1 200 OK\r\n"
				  "Content-Length: %zu\r\n"
				  "Connection: close\r\n"
				  "\r\n",
				  conn->file_size);
	conn->send_pos = 0;
}

static void connection_prepare_send_404(struct connection *conn)
{
	conn->send_len = snprintf(conn->send_buffer, BUFSIZ,
				  "HTTP/1.1 404 Not Found\r\n"
				  "Content-Length: 0\r\n"
				  "Connection: close\r\n"
				  "\r\n");
	conn->send_pos = 0;
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	const char *path = conn->request_path;

	if (strncmp(path, "/" AWS_REL_STATIC_FOLDER,
		    strlen("/" AWS_REL_STATIC_FOLDER)) == 0)
		return RESOURCE_TYPE_STATIC;
	if (strncmp(path, "/" AWS_REL_DYNAMIC_FOLDER,
		    strlen("/" AWS_REL_DYNAMIC_FOLDER)) == 0)
		return RESOURCE_TYPE_DYNAMIC;

	return RESOURCE_TYPE_NONE;
}

struct connection *connection_create(int sockfd)
{
	struct connection *conn = calloc(1, sizeof(*conn));

	DIE(conn == NULL, "calloc");

	conn->sockfd = sockfd;
	conn->fd = -1;
	conn->eventfd = -1;
	conn->state = STATE_INITIAL;
	conn->res_type = RESOURCE_TYPE_NONE;

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	size_t remaining = conn->file_size - conn->file_pos;
	int rc;

	conn->async_read_len = remaining < BUFSIZ ? remaining : BUFSIZ;

	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer,
		      conn->async_read_len, conn->file_pos);
	io_set_eventfd(&conn->iocb, conn->eventfd);
	conn->piocb[0] = &conn->iocb;

	rc = io_submit(conn->ctx, 1, conn->piocb);
	DIE(rc != 1, "io_submit");

	/* Stop watching the socket, wait for the async read notification. */
	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr");
	rc = w_epoll_add_ptr_in(epollfd, conn->eventfd, conn);
	DIE(rc < 0, "w_epoll_add_ptr_in");

	conn->state = STATE_ASYNC_ONGOING;
}

void connection_remove(struct connection *conn)
{
	if (conn->state == STATE_ASYNC_ONGOING)
		w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	else
		w_epoll_remove_ptr(epollfd, conn->sockfd, conn);

	if (conn->ctx)
		io_destroy(conn->ctx);
	if (conn->eventfd >= 0)
		close(conn->eventfd);
	if (conn->fd >= 0)
		close(conn->fd);
	close(conn->sockfd);

	conn->state = STATE_CONNECTION_CLOSED;
	free(conn);
}

void handle_new_connection(void)
{
	int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc;

	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	if (sockfd < 0) {
		ERR("accept");
		return;
	}

	dlog(LOG_INFO, "Accepted connection from: %s:%d\n",
	     inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

	rc = fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL, 0) | O_NONBLOCK);
	DIE(rc < 0, "fcntl");

	conn = connection_create(sockfd);

	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_ptr_in");

	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;
}

void receive_data(struct connection *conn)
{
	ssize_t bytes_recv;

	conn->state = STATE_RECEIVING_DATA;

	while (conn->recv_len < BUFSIZ - 1) {
		bytes_recv = recv(conn->sockfd, conn->recv_buffer + conn->recv_len,
				  BUFSIZ - 1 - conn->recv_len, 0);
		if (bytes_recv < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				return;
			goto remove_connection;
		}
		if (bytes_recv == 0)
			goto remove_connection;

		conn->recv_len += bytes_recv;
		conn->recv_buffer[conn->recv_len] = '\0';

		/* Stop once the whole request header has arrived. */
		if (strstr(conn->recv_buffer, "\r\n\r\n") != NULL)
			break;
	}

	conn->state = STATE_REQUEST_RECEIVED;
	return;

remove_connection:
	dlog(LOG_INFO, "Connection closed\n");
	conn->state = STATE_CONNECTION_CLOSED;
}

int connection_open_file(struct connection *conn)
{
	struct stat file_stat;
	int rc;

	if (strstr(conn->request_path, "..") != NULL)
		return -1;

	rc = snprintf(conn->filename, BUFSIZ, "%s%s",
		      AWS_DOCUMENT_ROOT, conn->request_path + 1);
	if (rc < 0 || rc >= BUFSIZ)
		return -1;

	conn->fd = open(conn->filename, O_RDONLY);
	if (conn->fd < 0)
		return -1;

	if (fstat(conn->fd, &file_stat) < 0 || !S_ISREG(file_stat.st_mode)) {
		close(conn->fd);
		conn->fd = -1;
		return -1;
	}

	conn->file_size = file_stat.st_size;
	conn->file_pos = 0;

	return conn->fd;
}

void connection_complete_async_io(struct connection *conn)
{
	struct io_event event;
	eventfd_t value;
	int rc;

	rc = eventfd_read(conn->eventfd, &value);
	if (rc < 0)
		return;

	rc = io_getevents(conn->ctx, 1, 1, &event, NULL);
	DIE(rc != 1, "io_getevents");

	rc = w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr");
	rc = w_epoll_add_ptr_out(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_add_ptr_out");

	if ((long)event.res <= 0) {
		/* Read error or unexpected EOF: nothing more to send. */
		conn->state = STATE_DATA_SENT;
		connection_remove(conn);
		return;
	}

	conn->send_len = event.res;
	conn->send_pos = 0;
	conn->state = STATE_SENDING_DATA;
}

int parse_header(struct connection *conn)
{
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};
	size_t parsed;

	conn->request_parser.data = conn;
	parsed = http_parser_execute(&conn->request_parser, &settings_on_path,
				     conn->recv_buffer, conn->recv_len);
	if (parsed != conn->recv_len || !conn->have_path)
		return -1;

	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	off_t offset = conn->file_pos;
	ssize_t rc;

	while (conn->file_pos < conn->file_size) {
		rc = sendfile(conn->sockfd, conn->fd, &offset,
			      conn->file_size - conn->file_pos);
		if (rc < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				return STATE_SENDING_DATA;
			return STATE_CONNECTION_CLOSED;
		}
		if (rc == 0)
			return STATE_CONNECTION_CLOSED;

		conn->file_pos = offset;
	}

	return STATE_DATA_SENT;
}

int connection_send_data(struct connection *conn)
{
	size_t total = 0;
	ssize_t rc;

	while (conn->send_pos < conn->send_len) {
		rc = send(conn->sockfd, conn->send_buffer + conn->send_pos,
			  conn->send_len - conn->send_pos, 0);
		if (rc < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			return -1;
		}
		if (rc == 0)
			return -1;

		conn->send_pos += rc;
		total += rc;
	}

	return total;
}

int connection_send_dynamic(struct connection *conn)
{
	if (connection_send_data(conn) < 0)
		return -1;

	/* Wait for the socket to become writable again. */
	if (conn->send_pos < conn->send_len)
		return 0;

	conn->file_pos += conn->send_len;
	if (conn->file_pos >= conn->file_size) {
		conn->state = STATE_DATA_SENT;
		return 0;
	}

	connection_start_async_io(conn);

	return 0;
}

static void connection_prepare_reply(struct connection *conn)
{
	int rc;

	if (parse_header(conn) == 0)
		conn->res_type = connection_get_resource_type(conn);

	if (conn->res_type == RESOURCE_TYPE_NONE ||
	    connection_open_file(conn) < 0) {
		connection_prepare_send_404(conn);
		conn->state = STATE_SENDING_404;
	} else {
		connection_prepare_send_reply_header(conn);
		conn->state = STATE_SENDING_HEADER;
	}

	if (conn->state == STATE_SENDING_HEADER &&
	    conn->res_type == RESOURCE_TYPE_DYNAMIC) {
		conn->eventfd = eventfd(0, EFD_NONBLOCK);
		DIE(conn->eventfd < 0, "eventfd");
		rc = io_setup(1, &conn->ctx);
		DIE(rc < 0, "io_setup");
	}

	rc = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_update_ptr_out");
}

void handle_input(struct connection *conn)
{
	switch (conn->state) {
	case STATE_INITIAL:
	case STATE_RECEIVING_DATA:
		receive_data(conn);
		if (conn->state == STATE_CONNECTION_CLOSED)
			connection_remove(conn);
		else if (conn->state == STATE_REQUEST_RECEIVED)
			connection_prepare_reply(conn);
		break;
	case STATE_ASYNC_ONGOING:
		connection_complete_async_io(conn);
		break;
	default:
		ERR("Unexpected state\n");
		break;
	}
}

void handle_output(struct connection *conn)
{
	switch (conn->state) {
	case STATE_SENDING_404:
		if (connection_send_data(conn) < 0 ||
		    conn->send_pos == conn->send_len) {
			conn->state = STATE_404_SENT;
			connection_remove(conn);
		}
		break;
	case STATE_SENDING_HEADER:
		if (connection_send_data(conn) < 0) {
			connection_remove(conn);
			break;
		}
		if (conn->send_pos < conn->send_len)
			break;

		conn->state = STATE_HEADER_SENT;
		if (conn->file_size == 0)
			connection_remove(conn);
		else if (conn->res_type == RESOURCE_TYPE_DYNAMIC)
			connection_start_async_io(conn);
		else
			conn->state = STATE_SENDING_DATA;
		break;
	case STATE_SENDING_DATA:
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			conn->state = connection_send_static(conn);
			if (conn->state != STATE_SENDING_DATA)
				connection_remove(conn);
		} else {
			if (connection_send_dynamic(conn) < 0 ||
			    conn->state == STATE_DATA_SENT)
				connection_remove(conn);
		}
		break;
	default:
		ERR("Unexpected state\n");
		break;
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	if (event & EPOLLIN) {
		dlog(LOG_DEBUG, "New message\n");
		handle_input(conn);
		return;
	}

	if (event & (EPOLLERR | EPOLLHUP)) {
		connection_remove(conn);
		return;
	}

	if (event & EPOLLOUT) {
		dlog(LOG_DEBUG, "Ready to send message\n");
		handle_output(conn);
	}
}

int main(void)
{
	int rc;

	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	listenfd = tcp_create_listener(AWS_LISTEN_PORT, BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	/* The listening socket is the only one registered with a NULL pointer. */
	rc = w_epoll_add_ptr_in(epollfd, listenfd, NULL);
	DIE(rc < 0, "w_epoll_add_ptr_in");

	dlog(LOG_INFO, "Server waiting for connections on port %d\n",
	     AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		rc = w_epoll_wait_infinite(epollfd, &rev);
		if (rc < 0) {
			if (errno == EINTR)
				continue;
			DIE(1, "w_epoll_wait_infinite");
		}

		if (rev.data.ptr == NULL) {
			dlog(LOG_DEBUG, "New connection\n");
			if (rev.events & EPOLLIN)
				handle_new_connection();
			continue;
		}

		handle_client(rev.events, rev.data.ptr);
	}

	return 0;
}
