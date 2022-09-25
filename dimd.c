#include<dirent.h>
#include<errno.h>
#include<fcntl.h>
#include<netinet/in.h>
#include<poll.h>
#include<pthread.h>
#include<signal.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/socket.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<sys/wait.h>
#include<unistd.h>
#include<limits.h>

#define MAXCONNECT 8
#define MAXBUFFER 8

typedef struct buffer_t buffer_t;
typedef struct buf_t buf_t;
typedef struct thread_info thread_t;
typedef struct connections_t connections_t;
typedef struct update_t update_t;

struct update_t {
	size_t linenum;
	int buf_id;
	char *line;
	char operation;
	pthread_t caller;
};

struct buf_t {
	char   *line;
	buf_t  *next;
	buf_t  *prev;
	int     linenum;
};

struct buffer_t {
	char      *name;
	buf_t     *head;
	int        id;
	pthread_t connections[MAXCONNECT];
	pthread_mutex_t mutex;
};

struct thread_info {			 
	pthread_mutex_t mutex;
	pthread_t       thread_id;	 
	int 			sock;
	int 			log;
};

struct connections_t {
	int n;
	pthread_mutex_t mutex;
};

static thread_t threads[MAXCONNECT];
static buffer_t buffers[MAXBUFFER];
static pthread_t MAIN_THREAD;

static const int C_LIST 	= 0x01; // list buffers
static const int C_CONN 	= 0x02; // connect to buffer
static const int C_OPEN 	= 0x04; // open buffer
static const int C_CLOS 	= 0x08; // close buffer
static const int C_TERM 	= 0x10; // terminate shim daemon

static connections_t nconnections; // needs to be accessed when threads terminate
static connections_t nbuffers; // needs to be accessed when requesting buffer

void handle_connected_buffer(buffer_t *buf, int sock);
void *handle_connection(void *th);

void signal_handler(int signo, siginfo_t *si, void *ucontext) {
	// I dont think this is doing anything
	if(signo == SIGTERM) {
		// TODO: will need to free buffers when we create them
		for(int i = 0; i < MAXCONNECT; i++) {
			int ret = pthread_mutex_trylock(&threads[i].mutex);
			if(ret == 0) {
				pthread_mutex_unlock(&threads[i].mutex);
			} else if(ret == EBUSY) {
				pthread_kill(threads[i].thread_id, SIGUSR1);
			} else {
				// all is lost, abandon all hope (and program execution)
				exit(1);
			}
		}
		exit(0);

	} else if(signo == SIGUSR1) {
		// recieve update struct as si.ptr
		pthread_t id = pthread_self();
		siginfo_t siginfo = *si;
		update_t update = *(update_t*)siginfo.si_ptr;
		for(int i = 0; i < MAXCONNECT; i++) {
			if(threads[i].thread_id == update.caller) continue;
			if(buffers[update.buf_id].connections[i] == 0) continue;
			send(threads[i].sock, &update.operation, 1, MSG_OOB);
			send(threads[i].sock, &update.linenum, sizeof(size_t), 0);
			if(update.line != NULL) send(threads[i].sock, update.line, strlen(update.line), 0);
		}

	} else if(signo == SIGUSR2) {
		siginfo_t siginfo = *si;
		pthread_t joinme = *(pthread_t*)siginfo.si_ptr;
		pthread_join(joinme, NULL);
	}
}

int main(void) {
		
	buffer_t buflist[MAXBUFFER];

	signal(SIGPIPE, SIG_IGN);
	struct sigaction sigact;
	sigact.sa_flags = SA_SIGINFO;
	sigact.sa_sigaction = &signal_handler;
	if(sigaction(SIGTERM, &sigact, NULL) < 0) { perror("sigaction"); exit(1); }
	if(sigaction(SIGUSR1, &sigact, NULL) < 0) { perror("sigaction"); exit(1); }
	if(sigaction(SIGUSR2, &sigact, NULL) < 0) { perror("sigaction"); exit(1); }
	const char * const logfile = "log";
	int log = open(logfile, O_WRONLY | O_CREAT | O_SYNC, S_IRUSR | S_IWUSR);
	if(log < 0) { perror("open log file"); exit(1); }
	ftruncate(log, 0);

	// daemonize: change to root dir, redirect stdout/stdin to dev/null
	//if( (daemon(0, 0)) < 0) { perror("daemon"); exit(1); }

	MAIN_THREAD = pthread_self();

	char test_message[] = "testing log file\n";
	write(log, test_message, strlen(test_message));

	// open a socket -- AF_INET = ipv4, AF_INET6 = ipv6, AF_UNIX = local unix connection
	int sock = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in address;
	const int port = 9025;
	socklen_t socklen = sizeof(address);
	const int one = 1;

	// set reauseto avoid keeping socket alive when restarting server
	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, socklen );
	if(sock == -1) { char errmsg[] = "failed to open socket; exiting\n"; write(log, errmsg, strlen(errmsg)); exit(1); }

	address.sin_family = AF_INET;
	address.sin_port = htons(port);
	address.sin_addr.s_addr = INADDR_ANY;

	// binds socket to address
	if( (bind(sock, (struct sockaddr*) &address, sizeof(address)) < 0) ) {
		char errmsg[] = "error binding to socket: ";
		write(log, errmsg, strlen(errmsg));
		char *serror = strerror(errno);
		write(log, serror, strlen(serror));
		write(log, "; exiting\n", 10);
		exit(1);
	}

	// listen on socket: up to backlog of 5; initiates ability to accept new connections
	listen(sock, 5);

	// will be used to get client address in accept()
	struct sockaddr_in cli_address;
	socklen_t clilen = sizeof(cli_address);

	//thread_t threads[MAXCONNECT]; // needs to be global to attempt cancellation

	for(int i = 0; i < MAXCONNECT; i++) {
		pthread_mutex_init(&threads[i].mutex, NULL);
	}
	for(int i = 0; i < MAXBUFFER; i++) {
		pthread_mutex_init(&buffers[i].mutex, NULL);
	}
	nconnections.n = 0;
	pthread_mutex_init(&nbuffers.mutex, NULL);
	pthread_mutex_init(&nconnections.mutex, NULL);

	while(1) {

		// accept blocks until a connection
		int newsock = accept(sock, (struct sockaddr*) &cli_address, &clilen);
		if(newsock < 0) {
			char errmsg[] = "error getting client socket; exiting\n"; 
			write(log, errmsg, strlen(errmsg));
			if(errno == EINTR) continue;
			break; // this shouldn't happen
		}

		for(int i = 0; i < MAXCONNECT; i++) {
			if(pthread_mutex_trylock(&threads[i].mutex) == EBUSY) {
				continue;
			} else {
				threads[i].sock = newsock;
				threads[i].log = log;
				int ret = pthread_create(&threads[i].thread_id, NULL, handle_connection, (void*)&threads[i]);
				if(ret < 0) { perror("pthread_create"); exit(1); } else { break; }
			}
		}
	}

	close(log);
	close(sock);

	return 0;
}

void *handle_connection(void *th) {
	// don't send sigusr to yourself
	thread_t *thread = th;
	sigset_t mask;
	pthread_sigmask(SIG_SETMASK, NULL, &mask);
	sigaddset(&mask, SIGUSR2);
	int ret = pthread_sigmask(SIG_BLOCK, &mask, NULL);
	if(ret < 0) { perror("pthread_sigmask"); pthread_exit(0); }
	struct pollfd pfd;
	pfd.fd = thread->sock;
	pfd.events = POLLPRI;
	pthread_mutex_lock(&nconnections.mutex);
		if(nconnections.n < MAXCONNECT) {
			send(thread->sock, "A", 1, 0);
			nconnections.n++;
			pthread_mutex_unlock(&nconnections.mutex);
		} else {
			send(thread->sock, "N", 1, 0);
			pthread_mutex_unlock(&nconnections.mutex);
			union sigval sv;
			sv.sival_ptr = (void*) &thread->thread_id;
			pthread_mutex_unlock(&thread->mutex);
			sigqueue(getpid(), SIGUSR2, sv);
			pthread_exit(0);
		}

	while(1) {
		int timeout = INT_MAX;
		int ret = poll(&pfd, 1, timeout);
		// if(ret < 0) { TODO: something went wrong, handle error } // EINTR
		if(pfd.revents & POLLERR) {
			break;
		} else if(pfd.revents & POLLPRI) {
			char c = '\0';
			ssize_t nread = recv(thread->sock, &c, 1, MSG_OOB);
			if(c == 't') { 
				pthread_kill(MAIN_THREAD, SIGTERM);
			} else if(c == 'l') {
				pthread_mutex_lock(&nbuffers.mutex);
				int n = nbuffers.n;
				pthread_mutex_unlock(&nbuffers.mutex);
				if(n > 0) {
					send(thread->sock, "A", 1, 0);
					char bufferlist[1024]; // TODO: Expand this dynamically
					memset(bufferlist, '\0', 1024);
					for(int i = 0; i < MAXBUFFER; i++) {
						pthread_mutex_lock(&buffers[i].mutex);
						if(buffers[i].name != NULL) {
							sprintf(bufferlist + strlen(bufferlist), "%d.\t%s\n", buffers[i].id, buffers[i].name); 
						}
						pthread_mutex_unlock(&buffers[i].mutex);
					}
					send(thread->sock, bufferlist, strlen(bufferlist), 0);
				} else {
					send(thread->sock, "N", 1, 0);
				}
				char reply = '\0';
				recv(thread->sock, &reply, 1, 0);
				break;
			} else if(c == 'a') {
				char buf_name[NAME_MAX];
				memset(buf_name, '\0', NAME_MAX);
				ssize_t nread = recv(thread->sock, buf_name, NAME_MAX-1, 0);
				pthread_mutex_lock(&nbuffers.mutex);
				if(nbuffers.n < MAXBUFFER) {
					nbuffers.n++;
					pthread_mutex_unlock(&nbuffers.mutex);
					ssize_t nwritten = send(thread->sock, "A", 1, 0);
					size_t file_size;
					ssize_t nread = recv(thread->sock, &file_size, sizeof(size_t), 0);
					char *buffer_tmp = calloc(1, file_size); 
					if(buffer_tmp == NULL)  /* need to log */ break;
					size_t len = 0;
					while(len < file_size) { nread = recv(thread->sock, buffer_tmp + len, file_size, 0); len += nread; } // TODO: error handling
					
					for(int i = 0; i < MAXBUFFER; i++) {
						if(pthread_mutex_trylock(&buffers[i].mutex) == EBUSY) continue;
						if(buffers[i].name == NULL) {
							buffers[i].name = calloc(1, 1+strlen(buf_name));
							buffers[i].id   = i;
							memcpy(buffers[i].name, buf_name, strlen(buf_name));
							buffers[i].head = calloc(1, sizeof(buf_t));
							buf_t *buf = buffers[i].head;
							
							char *line_start = buffer_tmp;
							char *line_end = buffer_tmp;
							size_t j = 0;
							size_t line = 0;
							for(size_t i = 0; i < file_size; i++) {
								j++;
								if(*line_end == '\n' || i+1 == file_size) {
									line++;
									buf->line = calloc(1, j);
									memcpy(buf->line, line_start, j);
									buf->linenum = line;
									buf->next = calloc(1, sizeof(buf_t));
									buf->next->prev = buf;
									buf = buf->next;
									j = 0;
									line_start = line_end+1;
								} 
								line_end++;
							}
							buf = buf->prev;
							free(buf->next);
							buf->next = NULL;
						pthread_mutex_unlock(&buffers[i].mutex);
						break;
						} else {
						pthread_mutex_unlock(&buffers[i].mutex);
						}
					}
				} else {
					pthread_mutex_unlock(&nbuffers.mutex);
					ssize_t nwritten = send(thread->sock, "N", 1, 0);
				}
				break;
			} else if(c == 'd') {
				pthread_mutex_lock(&nbuffers.mutex);
				int n = nbuffers.n;
				pthread_mutex_unlock(&nbuffers.mutex);
				if(n > 0) {
					ssize_t nwritten = send(thread->sock, "A", 1, 0);
					if(nwritten < 0) exit(1);
					int buf_close = -1;
					ssize_t nread = recv(thread->sock, &buf_close, sizeof(int), 0);
					if(nread < 0) break;
					if(buf_close < 0 || buf_close >= MAXBUFFER) { send(thread->sock, "Invalid buffer id\n", 18, 0); break; }
					pthread_mutex_lock(&buffers[buf_close].mutex);
					if(buffers[buf_close].name == NULL) { send(thread->sock, "Buffer does not exist\n", 22, 0); pthread_mutex_unlock(&buffers[buf_close].mutex); break; }
					buf_t *buf = buffers[buf_close].head->next;
					while(buf->next != NULL) {
						free(buf->prev);
						buf = buf->next;
					}
					free(buf);
					free(buffers[buf_close].name);
					buffers[buf_close].id = 0;
					buffers[buf_close].name = NULL;
					buffers[buf_close].head = NULL;
					pthread_mutex_unlock(&buffers[buf_close].mutex);
 					send(thread->sock, "Done\n", 5, 0);
					pthread_mutex_lock(&nbuffers.mutex);
					nbuffers.n--;
					pthread_mutex_unlock(&nbuffers.mutex);
					break;
				} else {
					ssize_t nwritten = send(thread->sock, "N", 1, 0);
					break;
				}
			} else if(c == 'c') {
				pthread_mutex_lock(&nbuffers.mutex);
				int n = nbuffers.n;
				pthread_mutex_unlock(&nbuffers.mutex);
				if(n > 0) {
					ssize_t nwritten = send(thread->sock, "A", 1, 0);
					int buf_id = 0;
					ssize_t nread = recv(thread->sock, &buf_id, sizeof(int), 0);
					if(buf_id < 0 || buf_id >= MAXBUFFER)  {
						nwritten = send(thread->sock, "Invalid buffer id\n", 18, 0); 
					} else {
						pthread_mutex_lock(&buffers[buf_id].mutex);
						if(buffers[buf_id].name == NULL) { 
							pthread_mutex_unlock(&buffers[buf_id].mutex);
							nwritten = send(thread->sock, "Buffer does not exist\n", 22, 0); 
						} else {
							pthread_mutex_unlock(&buffers[buf_id].mutex);
							nwritten = send(thread->sock, "Connection established\n", 23, 0); 
							handle_connected_buffer(&buffers[buf_id], thread->sock);
							break;
						}
					} 
					break;
				} else {
					ssize_t nwritten = send(thread->sock, "N", 1, 0);
					break;
				}
			} else if(c == 'q') {
				char msg[256] = { '\0' };
				pthread_mutex_lock(&nbuffers.mutex);
				int nbuff = nbuffers.n;
				pthread_mutex_unlock(&nbuffers.mutex);
				pthread_mutex_lock(&nconnections.mutex);
				int nconn = nconnections.n;
				pthread_mutex_unlock(&nconnections.mutex);
				snprintf(msg, 255, "buffers: %d\nconnections: %d\n", nbuff, nconn);
				send(thread->sock, msg, strlen(msg), 0);
				break;
			}
	 	} else if(pfd.revents & POLLHUP) {
			break;
		} else {
		}
	}
	pthread_mutex_lock(&nconnections.mutex);
	nconnections.n--;
	pthread_mutex_unlock(&nconnections.mutex);
	union sigval sv;
	sv.sival_ptr = (void*) &thread->thread_id;
	pthread_mutex_unlock(&thread->mutex);
	close(thread->sock);
	sigqueue(getpid(), SIGUSR2, sv);
	pthread_exit(NULL);
}

void handle_connected_buffer(buffer_t *buf, int sock) {

	char buf_content[1024];
	
	pthread_mutex_lock(&buf->mutex);
	ssize_t nwritten = send(sock, buf->name, NAME_MAX-1, 0);
	ssize_t buflen = 0;
	buf_t *buffer = buf->head;
	while(buffer!=NULL) {
		buflen += sprintf(buf_content + buflen, "%s", buffer->line);
		buffer = buffer->next;
		// TODO: send chunks until large files are entirely sent
	}
	for(int i = 0; i< MAXCONNECT; i++) {
		if(buf->connections[i] != 0) continue;
		buf->connections[i] = pthread_self();
		break;
	}
	pthread_mutex_unlock(&buf->mutex);

	send(sock, buf_content, buflen, 0);

	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = POLLPRI;

	while(1) {
		int timeout = INT_MAX;
		int ret = poll(&pfd, 1, timeout);
		if(ret < 0) { return; } // should handle better
		if(pfd.revents & POLLERR || pfd.revents & POLLHUP) {
			return;
		} else if(pfd.revents & POLLPRI) {
			char c;
			ssize_t nread = recv(sock, &c, 1, MSG_OOB);
			if(nread < 0) { return; }
			if(c == 'a') {
				// TODO: appending after last line segfaults
				size_t line = 0;
				ssize_t linelen = 0;
				nread = recv(sock, &line, sizeof(size_t), 0);
				nread = recv(sock, &linelen, sizeof(ssize_t), 0);
				if(nread < 0) { return; }
				char *newline = calloc(1, linelen+1);
				recv(sock, newline, linelen, 0);
				if(pthread_mutex_lock(&buf->mutex) < 0) { perror("mutex_lock"); exit(1); };
				buf_t *line_ptr = buf->head;
				while(line_ptr->linenum < line) { line_ptr = line_ptr->next; }
				buf_t *tmp = calloc(1, sizeof(buf_t));
				tmp->line = newline;
				tmp->next = line_ptr;
				tmp->prev = line_ptr->prev;
				line_ptr->prev->next = tmp;
				line_ptr->prev = tmp;
				while(line_ptr != NULL) {
					line_ptr->linenum++;
					line_ptr = line_ptr->next;
				}
				if (pthread_mutex_unlock(&buf->mutex) < 0) { perror("mutex_lock"); exit(1); };
				update_t update;
				update.linenum = line;
				update.line = newline;
				update.buf_id = buf->id;
				update.operation = 'a';
				update.caller = pthread_self();
				union sigval sv;
				sv.sival_ptr = (void*) &update;
				sigqueue(getpid(), SIGUSR1, sv);
				send(sock, "A", 1, 0);
				
			} else if(c == 'i') {
				size_t line = 0;
				ssize_t linelen = 0;
				nread = recv(sock, &line, sizeof(size_t), 0);
				nread = recv(sock, &linelen, sizeof(ssize_t), 0);
				if(nread < 0) { return; }
				char *newline = calloc(1, linelen+1);
				recv(sock, newline, linelen, 0);
				if(pthread_mutex_lock(&buf->mutex) < 0) { perror("mutex_lock"); exit(1); };
				buf_t *line_ptr = buf->head;
				while(line_ptr->linenum < line) { line_ptr = line_ptr->next; }
				buf_t *tmp = calloc(1, sizeof(buf_t));
				tmp->line = newline;
				tmp->linenum = line;
				tmp->next = line_ptr;
				tmp->prev = line_ptr->prev;
				if(line_ptr->prev == NULL) {
					buf->head = tmp;
				} else {
					line_ptr->prev->next = tmp;
				}
				line_ptr->prev = tmp;
				while(line_ptr != NULL) {
					line_ptr->linenum++;
					line_ptr = line_ptr->next;
				}
				if (pthread_mutex_unlock(&buf->mutex) < 0) { perror("mutex_lock"); exit(1); };
				update_t update;
				update.linenum = line;
				update.line = newline;
				update.buf_id = buf->id;
				update.operation = 'a';
				update.caller = pthread_self();
				union sigval sv;
				sv.sival_ptr = (void*) &update;
				sigqueue(getpid(), SIGUSR1, sv);
				send(sock, "A", 1, 0);

			} else if(c == 'd') {
				// TODO: line deletion segfaults
				size_t line = 0;
				recv(sock, &line, sizeof(size_t), 0);
				if(pthread_mutex_lock(&buf->mutex) < 0) { perror("mutex_lock"); exit(1); };
				buf_t *tmp = buf->head;
				while(tmp->linenum < line) { tmp = tmp->next; }
				if(tmp->line != NULL) free(tmp->line);
				if(tmp->prev == NULL) buf->head = tmp->next;
				if(tmp->next == NULL) tmp->prev->next = NULL;
				buf_t *dec = tmp->next;
				if(tmp != NULL) free(tmp);
				while(dec != NULL) {
					dec->line--;
					dec = dec->next;
				}
				if (pthread_mutex_unlock(&buf->mutex) < 0) { perror("mutex_lock"); exit(1); };
				update_t update;
				update.linenum = line;
				update.line = NULL;
				update.buf_id = buf->id;
				update.operation = 'd';
				update.caller = pthread_self();
				union sigval sv;
				sv.sival_ptr = (void*) &update;
				sigqueue(getpid(), SIGUSR1, sv);
				send(sock, "A", 1, 0);

			} else if(c == 'q') {
				pthread_t id = pthread_self();
				pthread_mutex_lock(&buf->mutex);
				for(int i = 0; i < MAXCONNECT; i++) {
					if(buf->connections[i] != id) continue;
					buf->connections[i] = 0;
				}
				pthread_mutex_unlock(&buf->mutex);
				return;
			}
		}

	}
	return;
}
