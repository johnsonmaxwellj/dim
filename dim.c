#include<netinet/in.h> // structure for storing address information
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<fcntl.h>
#include<string.h>
#include<limits.h>
#include<signal.h>
#include<errno.h>
#include<pthread.h>
#include<poll.h>
#include<sys/socket.h> // for socket APIs
#include<sys/types.h>
#include<sys/stat.h>

static const int C_LIST = 0x01; // list buffers
static const int C_CONN = 0x02; // connect to buffer
static const int C_OPEN = 0x04; // open buffer
static const int C_CLOS = 0x08; // close buffer
static const int C_TERM = 0x10; // terminate shim daemon
static const int ED_LOC = 0x20; // edit locally -- should be default operation
static const int C_QUER = 0x40; // query daemon

void parse_args(int argc, char **argv, int *flags);
void print_usage();
void handle_connected_buffer(int sock);
void *thread_poll(void *socket);
void signal_handler(int signo, siginfo_t *si, void *ucontext);

static const char prompt = ':';

typedef struct buf_t buf_t;
struct buf_t {
	buf_t *next;
	buf_t *prev;
	char *line;
	size_t linenum;
};

static pthread_t MAIN_THREAD;
static pthread_mutex_t buf_mutex;
static buf_t *buf_head;

int main(int argc, char **argv) {
	int flags = 0;
	parse_args(argc, argv, &flags);

	// temp hardcodes
	const char const filepath[] = "filedir/testfile";
	const char fname[] = "testfile";
	const int buf_close = 0;
	const int buf_conn = 0;
	int file = open(filepath, O_RDONLY);
	struct stat statbuf;
	if(stat(filepath, &statbuf) < 0) { perror("stat"); exit(1); }
	char *file_content = calloc(1, 1+statbuf.st_size);
	if(!file_content) { perror("calloc"); exit(1); }
	ssize_t nread = read(file, file_content, statbuf.st_size);
	if(nread < 0) { perror("read"); exit(1); }
	close(file);

	if(flags == 0) {
		printf("this should open an unnamed buffer\n");
	} else if(flags == ED_LOC) {
	} else { 
		// server connection
		int sock = socket(AF_INET, SOCK_STREAM, 0);
		int port = 9025;
		struct sockaddr_in address;
		address.sin_family = AF_INET;
		address.sin_port = htons(port);
		address.sin_addr.s_addr = INADDR_ANY;
		printf("connecting to socket...\n"); // DEBUG
		int connection = connect(sock, (struct sockaddr*)&address, sizeof(address));
		printf("...done. connected to socket: %d\n\n", sock); // DEBUG
		if (connection == -1) {
			perror("connect");
			exit(1);
		}

		char reply;
		ssize_t nread = recv(sock, &reply, 1, 0);
		if(nread < 0) { perror("recv"); exit(1); }
		if(reply != 'A') { printf("Max connection limit has been reached. -- See MAXCONNECT\n"); exit(0); }


		if(flags == C_LIST) {
			ssize_t nwritten = send(sock, "l", 1, MSG_OOB);
			if(nwritten < 0) { perror("send"); exit(1); }
			char reply;
			ssize_t nread = recv(sock, &reply, 1, 0);
			if(nread < 0) { perror("recv"); exit(1); }
			if(reply == 'A') {
				char buffer_list[1024] = { '\0' };
				printf("reading\n");
				nread = recv(sock, buffer_list, 1023, 0); // need to do checking to see if buffer is full; reasonably shouldn't be for now
				printf("strlen: %d | nread: %d\n", strlen(buffer_list), nread);
				if(nread < 0) { perror("recv"); exit(1); }
				printf("Available buffers:\n");
				printf("%s", buffer_list);
			} else { 
				printf("No buffers available\n");
			}
			nwritten = send(sock, "l", 1, MSG_OOB);
		} else if(flags == C_CONN) {
			ssize_t nwritten = send(sock, "c", 1, MSG_OOB);
			if(nwritten < 0) { perror("send"); exit(1); }
			char reply;
			ssize_t nread = recv(sock, &reply, 1, 0);
			if(nread < 0) { perror("recv"); exit(1); }
			if(reply == 'A') {
				nwritten = send(sock, &buf_conn, sizeof(int), 0);
				char response[128] = { '\0' };
				nread = recv(sock, response, 127, 0);
				if(nread < 0) { perror("recv"); exit(1); }
				if(strncmp(response, "Invalid buffer id\n", nread) == 0) {
					printf("%s", response);
					exit(0);
				} else if(strncmp(response, "Buffer does not exist\n", nread) == 0) {
					printf("%s", response);
					exit(0);
				} else if(strncmp(response, "Connection established\n", nread) == 0) {
					printf("%s", response);
					handle_connected_buffer(sock);
					exit(0);
				} else {
					printf("Unknown response '%s' from server; panic\n", response);
					exit(1);
				}
			} else {
				printf("No buffers available\n");
			}
		} else if(flags == C_OPEN) {
			ssize_t nwritten = send(sock, "a", 1, MSG_OOB);
			if(nwritten < 0) { perror("send"); exit(1); }
			nwritten = send(sock, fname, strlen(fname), 0);
			if(nwritten < 0) { perror("send"); exit(1); }
			char reply = '\0';
			ssize_t nread = recv(sock, &reply, 1, 0);
			if(nread < 0) { perror("recv"); exit(1); }
			if(reply == 'A') {
			nwritten = send(sock, &statbuf.st_size, sizeof(size_t), 0);
			if(nwritten < 0) { perror("send"); exit(1); }
			send(sock, file_content, statbuf.st_size, 0);
			if(nwritten < 0) { perror("send"); exit(1); }
			} else {
				printf("Max buffer limit has been reached -- See MAXBUFFER\n");
			}
			exit(0);
		} else if(flags == C_CLOS) {
			ssize_t nwritten = send(sock, "d", 1, MSG_OOB);
			if(nwritten < 0) { perror("send"); exit(1); }
			char reply = '\0';
			ssize_t nread = recv(sock, &reply, 1, 0);
			if(nread < 0) { perror("recv"); exit(1); }
			if(reply == 'A') {
				nwritten = send(sock, &buf_close, sizeof(int), 0);
				char response[128] = { '\0' };
				nread = recv(sock, response, 127, 0);
				if(nread < 0) { perror("read"); exit(1); }
				if(strncmp("Invalid buffer id\n", response, nread) == 0) {
					printf("%s", response);
				} else if(strncmp("Buffer does not exist\n", response, nread) == 0) {
					printf("%s", response);
				} else if(strncmp("Done\n", response, nread) == 0) {
					printf("%s", response);
				} else {
					printf("Unknown response '%s' from daemon; panic\n", response);
					exit(1);
				}
			} else {
				printf("No buffers available\n");
			}
		} else if(flags == C_TERM) {
			ssize_t nwritten = send(sock, "t", 1, MSG_OOB);
			if(nwritten < 0) { perror("send"); exit(1); }
		} else if(flags == C_QUER) {
			ssize_t nwritten = send(sock, "q", 1, MSG_OOB);
			char query[256] = { '\0' };
			nread = recv(sock, query, 255, 0);
			printf("%s", query);
			exit(0);
		} else {
			printf("flag unhandled -- hit else; should not be reachable\n");
			exit(1);
		}

		close(sock);
	}

	return 0;
}

void parse_args(int argc, char **argv, int *flags) {
	// TODO: open blank buffer for editing on argc == 1
	if(argc == 1) {
		*flags = 0;
		return;
	} 

	const char help[]           = "-h";
	const char list[]           = "-l";
	const char add[]            = "-a";
	const char connect[]        = "-c";
	const char terminate[]      = "-t";
	const char delete[]         = "-d";
	const char add_long[]       = "--add";
	const char help_long[]      = "--help";
	const char list_long[]      = "--list";
	const char delete_long[]    = "--delete";
	const char connect_long[]   = "--connect";
	const char terminate_long[] = "--terminate";

	const char query[] 			= "-q";

	for(int i = 1; i < argc; i++) {
		char *str = argv[i];
		int len = strlen(str);
		if(str[0] == '-') {
			if(str[1] == '-') {
				// TODO: match long options
				printf("matching long options not implemented yet\n");
				exit(0);
			} else {
				if(str[1] == 'h') {
					print_usage();
					exit(0);
				} else if (str[1] == 'l') {
					*flags = *flags | C_LIST;
					return;
				} else if(str[1] == 'a') {
					// TODO: add buffer
					*flags = *flags | C_OPEN;
					return;
				} else if(str[1] == 'c') {
					*flags = *flags | C_CONN;
					return;
				} else if(str[1] == 't') {
					// TODO: terminate daemon
					*flags = *flags | C_TERM;
					return;
				} else if(str[1] == 'd') {
					*flags = *flags | C_CLOS;
					return;
				} else if(str[1] == 'q') {
					*flags = *flags | C_QUER;
					return;
				} else {
					printf("unknown option: %s\n\n", str);
					print_usage();
					exit(0);
				}
			}
		} else {
			struct stat sb;
			if( (stat(str, &sb) == -1) ) { 
				if(errno != ENOENT) { perror("stat"); exit(1); }
				printf("this should open a new named buffer\n");
				exit(0);
			} else {
				printf("this should open an existing file\n");
				exit(0);
			}
			*flags = *flags | ED_LOC;
			return;
		}
	}
	print_usage();
}

void print_usage() {
	printf("shim usage:\n"
		   "shim open empty buffer for editing\n\n"
		   "shim <file>\n"
		   "\topen <file> in shim for editing\n\n"
		   "shim [options] <file>\n"
		   "options:\n"
		   "\t-h, --help      \tprint this help message and exit\n"
		   "\t-a, --add       \tadd <file>\t\t add <file> as a buffer in shimd\n"
		   "\t-l, --list      \tlist buffers available in shimd\n"
		   "\t-c, --connect   \tconnect to <buffer> to buffer in shimd\n"
		   "\t-d, --delete    \tdelete <buffer> in shimd\n"
		   "\t-t, --terminate \tterminate shimd\n"
		  );
}

void signal_handler(int signo, siginfo_t *si, void *ucontext) {
	if(signo == SIGUSR2) {
		siginfo_t siginfo = *si;
		pthread_t joinme = *(pthread_t*)siginfo.si_ptr;
		pthread_join(joinme, NULL);
	} else if(signo == SIGUSR1) {

		// TODO: reprocess the buffer;
	} else {
		// this should not happen
		fprintf(stderr, "handling unexpected signal\n");
		exit(1);
	}

}
	
void handle_connected_buffer(int sock) {
		signal(SIGPIPE, SIG_IGN);
		struct sigaction sigact;
		sigact.sa_flags = SA_SIGINFO;
		sigact.sa_sigaction = &signal_handler;
		if(sigaction(SIGUSR1, &sigact, NULL) < 0) { perror("sigaction"); exit(1); }
		if(sigaction(SIGUSR2, &sigact, NULL) < 0) { perror("sigaction"); exit(1); }

		char buf_name[NAME_MAX] = { '\0' };
		ssize_t nread = recv(sock, buf_name, NAME_MAX-1, 0);
		char buf[1024] = { '\0' };
		ssize_t bufsize = recv(sock, buf, 1023, 0);
		if(bufsize < 0) { perror("recv"); exit(1); }



		char *line_start = buf;
		char *line_end   = buf;
		size_t j = 0;
		buf_head = calloc(1, sizeof(buf_t));
		buf_t *connected_buf = buf_head;
		size_t line = 0;
		for(size_t i = 0; i < bufsize; i++) {
			j++;
			if(*line_end == '\n' || i+1 == bufsize) {
				line++;
				connected_buf->line = calloc(1, j);
				memcpy(connected_buf->line, line_start, j);
				connected_buf->linenum = line;
				connected_buf->next = calloc(1, sizeof(buf_t));
				connected_buf->next->prev = connected_buf;
				connected_buf = connected_buf->next;
				j = 0;
				line_start = line_end+1;
			}
			line_end++;
		}
		connected_buf = connected_buf->prev;
		free(connected_buf->next);
		connected_buf->next = NULL;

		buf_t *curr_line = buf_head;

		MAIN_THREAD = pthread_self();
		pthread_t thread;
		int ret = pthread_create(&thread, NULL, thread_poll, (void*)&sock);
		if(ret < 0) { perror("pthread_create"); exit(1); }

		// TODO: track size of buffer to allocate mem for writing file in a single write
		// or possibly scatter write the file from each buf_t line
		while(1) {
			// TODO: fill an error mesage with diagnostic when command not completed
			char cmd[256] = { '\0' };
			char line_buf[256] = { '\0' };
			
			write(STDOUT_FILENO, &prompt, 1 );

			ssize_t nread = read(STDIN_FILENO, cmd, 255);

			if(nread < 0) { perror("read"); exit(1); }
			if(strncmp(cmd, "a\n", nread) == 0) {
				printf("append line\n");
				nread = read(STDIN_FILENO, &line_buf, 255);
				pthread_mutex_lock(&buf_mutex);
				buf_t *tmp = calloc(1, sizeof(buf_t));
				tmp->line = calloc(1, nread+1);
				memcpy(tmp->line, line_buf, nread);
				if(curr_line->next != NULL) {
					tmp->next = curr_line->next;
					tmp->prev = curr_line;
					curr_line->next = tmp;
					tmp->next->prev = tmp;
					while(tmp != NULL) {
						tmp->linenum = tmp->prev->linenum+1;
						tmp = tmp->next;
					}
				} else {
					tmp->prev = curr_line;
					curr_line->next = tmp;
					tmp->linenum = tmp->prev->linenum+1;
				}
				pthread_mutex_unlock(&buf_mutex);
				curr_line = curr_line->next;
				send(sock, "a", 1, MSG_OOB);
				send(sock, &curr_line->linenum, sizeof(size_t), 0);
				send(sock, &nread, sizeof(ssize_t), 0);
				send(sock, line_buf, nread, 0);
				char reply = '\0';
				nread = recv(sock, &reply, 1, 0); 
				if(reply != 'A') {
					printf("update not sent\n");
				}
			} else if(strncmp(cmd, "i\n", nread) == 0) {
				printf("insert line\n");
				nread = read(STDIN_FILENO, &line_buf, 255);
				pthread_mutex_lock(&buf_mutex);
				buf_t *tmp = calloc(1, sizeof(buf_t));
				tmp->line = calloc(1, nread+1);
				tmp->linenum = curr_line->linenum;
				memcpy(tmp->line, line_buf, nread);
				if(curr_line->prev != NULL) {
					tmp->next = curr_line;
					tmp->prev = curr_line->prev;
					curr_line->prev->next = tmp;
					curr_line->prev = tmp;
					while(tmp != NULL) {
						tmp->linenum = tmp->prev->linenum+1;
						tmp = tmp->next;
					}
				} else {
					tmp->next = buf_head;
					buf_head->prev = tmp;
					buf_head = tmp;
					tmp->linenum = 1;
					while(tmp->next != NULL) {
						tmp = tmp->next;
						tmp->linenum = tmp->prev->linenum+1;
					}
				}
				pthread_mutex_unlock(&buf_mutex);
				curr_line = curr_line->prev;
				send(sock, "i", 1, MSG_OOB);
				send(sock, &curr_line->linenum, sizeof(size_t), 0);
				send(sock, &nread, sizeof(ssize_t), 0);
				send(sock, line_buf, nread, 0);
				char reply = '\0';
				nread = recv(sock, &reply, 1, 0); 
				if(reply != 'A') {
					printf("update not sent\n");
				}
			} else if(strncmp(cmd, "d\n", nread) == 0) {
				buf_t *tmp = curr_line;
				char reply = '\0';
				pthread_mutex_lock(&buf_mutex);
				if(tmp->line != NULL) free(tmp->line);
				if(tmp->prev != NULL) {
					tmp->prev->next = tmp->next;
				} else {
					buf_head = tmp->next;
				}
				if(tmp->next != NULL) tmp->next->prev = tmp->prev;
				curr_line = tmp->next;
				if(tmp != NULL) free(tmp);
				tmp = curr_line;
				while(tmp != NULL) {
					tmp->linenum--;
					tmp = tmp->next;
				}
				pthread_mutex_unlock(&buf_mutex);
				send(sock, "d", 1, MSG_OOB);
				send(sock, &curr_line->linenum, sizeof(size_t), 0);
				recv(sock, &reply, 1, 0);

				if(reply != 'A') printf("update not sent\n");
			} else if(strncmp(cmd, "q\n", nread) == 0) {
				printf("exiting\n");
				send(sock, "q", 1, MSG_OOB);
				exit(0); 
			} else if(strncmp(cmd, "-\n", nread) == 0) {
				if(curr_line->prev == NULL) {
					printf("at first line\n");
				} else {
					curr_line = curr_line->prev;
				}
			} else if(strncmp(cmd, "+\n", nread) == 0) {
				if(curr_line->next == NULL) {
					printf("at last line\n");
				} else {
					curr_line = curr_line->next;
				}
			} else if(strncmp(cmd, "p\n", nread) == 0) {
				printf("%s", curr_line->line);
			} else if(strncmp(cmd, ".\n", nread) == 0) {
				printf("%s", curr_line->line);
			} else if(strncmp(cmd, "print buffer\n", nread) == 0) {
				buf_t *tmp = buf_head;
				while(tmp!= NULL) {
					printf("%d\t%s", tmp->linenum, tmp->line);
					tmp = tmp->next;
				}
			} else if(strncmp(cmd, "n\n", nread) == 0) {
				printf("%d\t%s", curr_line->linenum, curr_line->line);
			} else {
				printf("unrecognized command\n");
			}
			
		}
}

void *thread_poll(void *socket) {
	int *s = (int *)socket;
	int sock = *s;

	sigset_t mask;
	pthread_sigmask(SIG_SETMASK, NULL, &mask);
	sigaddset(&mask, SIGUSR2);
	int ret = pthread_sigmask(SIG_BLOCK, &mask, NULL);
	if(ret < 0) { perror("pthread_sigmask"); pthread_exit(NULL); }

	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = POLLPRI;

	while(1) {

		int timeout = INT_MAX;
		int ret = poll(&pfd, 1, timeout);
		if(ret < 0) { perror("poll"); break; }

		if(pfd.revents & POLLERR || pfd.revents & POLLHUP) {
			write(STDERR_FILENO, "connection interupted\n", 22);
			break;
		} else if(pfd.revents & POLLPRI) {
			char op = '\0';
			ssize_t nread = recv(sock, &op, 1, MSG_OOB);
			if(nread < 0) { perror("recv"); break; }
				if(op == 'c') {
					printf("\n[change line request]\n:");
					pthread_kill(MAIN_THREAD, SIGUSR2);
				} else if(op == 'a') {
 					const char msg[] = "\n[add line request]\n:";
					write(STDOUT_FILENO, msg, strlen(msg));
					size_t linenum = 0;
					recv(sock, &linenum, sizeof(size_t), 0);
					char line[256] = { '\0' };
					nread = recv(sock ,&line, 255, 0);
					buf_t *tmp = buf_head;
					pthread_mutex_lock(&buf_mutex);
					while(tmp->linenum < linenum) { tmp = tmp->next; }
					buf_t *newline = calloc(1, sizeof(buf_t));
					newline->line = calloc(1, 1+nread);
					memcpy(newline->line, line, nread);
					if(tmp->prev == NULL) {
						buf_head = newline;
					} else {
						tmp->prev->next = newline;
					}
					newline->next = tmp;
					newline->prev = tmp->prev;
					tmp->prev = newline;
					newline->linenum = linenum;
					while(tmp != NULL) {
						tmp->linenum++;
						tmp = tmp->next;
					}
					pthread_mutex_unlock(&buf_mutex);
				} else if(op == 'd'){
					const char msg[] = "\n[delete line request]\n:";
					write(STDOUT_FILENO, msg, strlen(msg));
					size_t linenum = 0;
					recv(sock, &linenum, sizeof(size_t), 0);
					pthread_mutex_lock(&buf_mutex);
					buf_t *tmp = buf_head;
					while(tmp->linenum < linenum) { tmp = tmp->next; }
					if(tmp->line != NULL) free(tmp->line);
					if(tmp->prev != NULL) tmp->prev->next = tmp->next;
					if(tmp->next != NULL) tmp->next->prev = tmp->prev;
					buf_t *dec = tmp->next;
					free(tmp);
					tmp = dec;
					while(dec != NULL) {
						dec->linenum--;
						dec = dec->next;
					}
					pthread_mutex_unlock(&buf_mutex);
				} else {
					fprintf(stderr, "unknown operation request\n");
					exit(1);
				}
		} else {
		}

	}

	union sigval sv;
	pthread_t id = pthread_self();
	sv.sival_ptr = (void*) id;
	close(sock);
	sigqueue(getpid(), SIGUSR1, sv);
	pthread_exit(NULL);
}
