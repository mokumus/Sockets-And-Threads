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

/*-----------------GLOBALS--------------------*/
sig_atomic_t exit_requested = 0;                          //SIGINT FLAG


pthread_mutex_t mutex_stdout = PTHREAD_MUTEX_INITIALIZER; // File lock mutex for stdout

int server_socket, // Server descriptor
    client_socket, // Client descriptor
    pool_size,     // Current thread pool size
    n_running;     // Currently busy threads

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

// Misc functions
void print_usage();
char *timestamp();
void cleanup();

// Signal handler
void sigint_handler(int sig_no);

int main(int argc, char *argv[])
{

  /*--------------Check for another instance--------------*/

  /*--------------Daemonize-------------------------------*/

  /* -------------Parse command line input ---------------*/

  /* -------------Check input validity -------------------*/

  /*--------------Initilize server------------------------*/

  /*--------------Main loop-------------------------------*/

  /*--------------Free resources--------------------------*/
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

void print_usage(void)
{
  printf("\n========================================\n");
  printf("Usage:\n"
         "./server [-p port] [-o path to log file] [-l pool size( L>= 2)] [-d dataset path]\n");
  printf("========================================\n");
}

char *timestamp()
{
  time_t now = time(NULL);
  char *time = asctime(gmtime(&now));
  time[strlen(time) - 1] = '\0'; // Remove \n
  return time;
}