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

#define MAX_PATH 1024   // Max file path length
#define MAX_BACKLOG 128 // Max server queue for listen()
#define MAX_FIELDS 30   // Max number of fields csv can have
#define MAX_NAME 64     // Max string lenght of field names
#define MAX_LINE 1024   // Max string of a data row(combined string length of all fields)

/*-----------------DATA STRUCTURE-------------*/

typedef struct
{
  int n_fields;
  int n_rows;
  char fields[MAX_FIELDS][MAX_NAME];
  char ***rows;
} DataBase;

/*-----------------GLOBALS--------------------*/
typedef struct
{
  int id;
  int active;
} ThreadData;

pthread_t *thread_ids; // Path calculator thread PIDS
ThreadData *td;
DataBase db;

sig_atomic_t exit_requested = 0; //SIGINT FLAG

pthread_mutex_t mutex_stdout = PTHREAD_MUTEX_INITIALIZER; // File lock mutex for stdout

int server_socket, // Server descriptor
    client_socket, // Client descriptor
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

// Database functions
int lines(const char *path);
DataBase *db_init(void);
void db_print(DataBase *db, int start, int end);
void db_print_row(DataBase *db, int n);

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

  /*--------------Initilize DB----------------------------*/

  DataBase *db = db_init();

  //db_print(db, 0, db->n_rows);

  for (int i = 0; i < db->n_rows; i++)
  {
    for (int k = 0; k < db->n_fields; k++)
      free(db->rows[i][k]);
    free(db->rows[i]);
  }
  free(db->rows);

  exit(EXIT_SUCCESS);

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

  // Free DB
  for (int i = 0; i < db->n_rows; i++)
  {
    for (int k = 0; k < db->n_fields; k++)
      free(db->rows[i][k]);
    free(db->rows[i]);
  }
  free(db->rows);

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

int lines(const char *path)
{
  int lines = 0;
  char buffer[MAX_LINE];
  FILE *fp = fopen(path, "r");

  while (fgets(buffer, sizeof(buffer), fp) != NULL)
    lines++;

  fclose(fp);

  return lines;
}

DataBase *db_init(void)
{
  DataBase static db;
  char line[MAX_LINE];
  char *token;
  int curr_line = 0;
  FILE *fp = fopen(_D, "r");

  // Get field names from the first row
  fgets(line, MAX_LINE, fp);
  token = strtok(line, ",");
  strcpy(db.fields[db.n_fields], token);
  while (token != NULL)
  {
    strcpy(db.fields[db.n_fields++], token);
    token = strtok(NULL, ",");
  }

  // Allocate memory for the database
  db.n_rows = lines(_D) - 1;
  db.rows = malloc(db.n_rows * sizeof(char **));
  for (int i = 0; i < db.n_rows; i++)
    db.rows[i] = malloc(db.n_fields * sizeof(char *));

  
  // Populate fields from the input file
  while (fgets(line, MAX_LINE, fp))
  {
    int in_quote = 0;
    int curr_field = 0;
    char buffer[256];
    int n = 0;

    for (int i = 0; line[i] != 0; i++)
    {
      if (line[i + 1] == '\n')
      {
        buffer[n] = 0;
        db.rows[curr_line][curr_field] = malloc((n + 1) * sizeof(char));
        strcpy(db.rows[curr_line][curr_field], buffer);
        continue;
      }

      if (line[i] == '"')
      {
        in_quote = in_quote == 0 ? 1 : 0;
        continue;
      }

      if (!in_quote)
      {
        if (line[i] == ',')
        {
          buffer[n] = 0;
          db.rows[curr_line][curr_field] = malloc((n + 1) * sizeof(char));
          strcpy(db.rows[curr_line][curr_field], buffer);
          curr_field++;
          n = 0;
        }

        else
          buffer[n++] = line[i];
      }
      else
      {
        buffer[n++] = line[i];
        if (line[i] == '"')
        {
          buffer[n] = 0;
          db.rows[curr_line][curr_field] = malloc((n + 1) * sizeof(char));
          strcpy(db.rows[curr_line][curr_field], buffer);
          n = 0;
        }
      }
    }
    curr_line++;
  }
  fclose(fp);
  return &db;
}

void db_print(DataBase *db, int start, int end)
{
  for (int i = start; i < end; i++)
    db_print_row(db, i);
}

void db_print_row(DataBase *db, int n)
{
  printf("R%d:", n);
  for (int i = 0; i < db->n_fields; i++)
    printf("%s,", db->rows[n][i]);
  printf("\n");
}