//
//  server.c
//  Threads & TCP Sockets
//
//  Created by Muhammed Okumus on 28.05.2021.
//  Copyright 2021 Muhammed Okumus. All rights reserved.
//  mokumus1996@gmail.com
//	151044017
//

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <string.h> // strlen()
#include <unistd.h> // getopt(), access(), read(), close()
#include <errno.h>  // error handling stuff, perror(), errno
#include <time.h>   // time()
#include <sys/socket.h>
#include <signal.h>
#include <pthread.h>
#include <netinet/in.h>
#include <math.h>
#include <limits.h>
#include <semaphore.h>

/* ----------------DEFINES--------------------*/

#define MAX_PATH 1024
#define MAX_BACKLOG 128 // Maximum server queue for listen()

/*-----------------GLOBALS--------------------*/
typedef struct
{
  int id;
  int active;
} ThreadData;

pthread_t *thread_ids; // Path calculator thread PIDS
ThreadData *td;

sig_atomic_t exit_requested = 0; //SIGINT FLAG

pthread_mutex_t mutex_stdout = PTHREAD_MUTEX_INITIALIZER; // File lock mutex for stdout

int server_socket, // Server descriptor
    client_socket, // Client descriptor
    pool_size,     // Current thread pool size
    n_running;     // Currently busy threads

int opt_P, _P, // Input port no
    opt_O,
    opt_L, _L,
    opt_D;

char _O[MAX_PATH]; // Log file input
char _D[MAX_PATH]; // Dataset file input

/* -------------------MACROS--------------------*/
// Exitting macro with a message
#define errExit(msg)    \
  do                    \
  {                     \
    exit_requested = 1; \
    perror(msg);        \
    exit(EXIT_FAILURE); \
  } while (0)
// Logging macro to add timestamp before prints
#define print_log(f_, ...) pthread_mutex_lock(&mutex_stdout), printf("%s ", timestamp()), printf((f_), ##__VA_ARGS__), printf("\n"), fflush(stdout), pthread_mutex_unlock(&mutex_stdout)

/* ----------------DATA STRUCTURE-------------*/

/* ----------------PROTOTYPES-----------------*/

void *worker_thread(void *data);

// Misc functions
void print_usage(void);
void print_inputs(void);
char *timestamp(void);
void cleanup(void);
void exit_on_invalid_input(void);

// Signal handler
void sigint_handler(int sig_no);

int main(int argc, char *argv[])
{
  int option; // Getopt buffer

  /*--------------Check for another instance--------------*/

  /*--------------Daemonize-------------------------------*/

  /* -------------Parse command line input ---------------*/
  while ((option = getopt(argc, argv, "p:o:l:d:")) != -1)
  {
    switch (option)
    {
    case 'p':
      opt_P = 1;
      _P = atoi(optarg);
      break;
    case 'o':
      opt_O = 1;
      snprintf(_O, MAX_PATH, "%s", optarg);
      break;
    case 'l':
      opt_L = 1;
      _L = atoi(optarg);
      break;
    case 'd':
      opt_D = 1;
      snprintf(_D, MAX_PATH, "%s", optarg);
      break;
    default:
      print_usage();
      exit(EXIT_FAILURE);
      break;
    }
  }
  /* -------------Check input validity -------------------*/

  exit_on_invalid_input();

  // Close open file descriptors after the parent exits

  // Open log file and redirect stdout and stderr

  print_inputs();

  /*--------------Initilize server------------------------*/
  setbuf(stdout, NULL);
  setbuf(stderr, NULL);

  struct sockaddr_in addr_server = {0};
  struct sockaddr_in addr_client = {0};
  socklen_t addr_len;
  int enable = 1;

  thread_ids = malloc(_L * sizeof(pthread_t));
  td = malloc(_L * sizeof(ThreadData));

  signal(SIGINT, sigint_handler);

  for (int i = 0; i < _L; i++)
  {
    td[i].id = i;
    td[i].active = 0;
    pthread_create(&thread_ids[i], NULL, worker_thread, &td[i]);
  }

  server_socket = socket(AF_INET, SOCK_STREAM, 0);
  if (server_socket == -1)
    errExit("Socket failed");

  // Free port at exit, don't wait for TIME_WAIT to release
  if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
    errExit("setsockopt(SO_REUSEADDR) failed");

  addr_server.sin_family = AF_INET;
  addr_server.sin_addr.s_addr = htonl(INADDR_ANY);
  addr_server.sin_port = htons(_P);

  if ((bind(server_socket, (struct sockaddr *)&addr_server, sizeof(addr_server))) == -1)
    errExit("Bind failed");

  if ((listen(server_socket, MAX_BACKLOG)) == -1)
    errExit("Listen failed");

  /*--------------Main loop-------------------------------*/
  // Handle connections while no SIGINT
  while (!exit_requested)
  {
    addr_len = sizeof(addr_client);
    client_socket = accept(server_socket, (struct sockaddr *)&addr_client, &addr_len);

    if (client_socket > 0)
    {

      int from, to;

      read(client_socket, &from, sizeof(int));
      read(client_socket, &to, sizeof(int));

      printf("TO: %d -- FROM: %d\n", to, from);

      write(client_socket, "CTRL+C Mühendisi\n", strlen("CTRL+C Mühendisi\n"));
    }
  }

  /*--------------Free resources--------------------------*/

  free(thread_ids);
  free(td);

  return 0;
}

void *worker_thread(void *data)
{
  pthread_detach(pthread_self());
  
  return NULL;
}

void cleanup()
{
  return;
}

void sigint_handler(int sig_no)
{
  exit_requested = 1;
  close(server_socket);
}

void exit_on_invalid_input(void)
{
  if (_L < 2)
  {
    printf("Number of threads at startup cannot be lower than 2\n");
    print_usage();
    exit(EXIT_FAILURE);
  }
  if (_P <= 1000)
  {
    printf("Port number should be larger then 1000.\n");
    print_usage();
    exit(EXIT_FAILURE);
  }
  if (!(opt_P && opt_O && opt_L && opt_D))
  {
    printf("Missing parameters\n");
    print_usage();
    exit(EXIT_FAILURE);
  }
}

void print_usage(void)
{
  printf("\n========================================\n");
  printf("Usage:\n"
         "./server [-p port] [-o path to log file] [-l pool size( L>= 2)] [-d dataset path]\n");
  printf("========================================\n");
}

void print_inputs(void)
{
  print_log("Executing with parameters:");
  print_log("-p %d", _P);
  print_log("-o %s", _O);
  print_log("-l %d", _L);
  print_log("-d %s", _D);
}

char *timestamp()
{
  time_t now = time(NULL);
  char *time = asctime(gmtime(&now));
  time[strlen(time) - 1] = '\0'; // Remove \n
  return time;
}