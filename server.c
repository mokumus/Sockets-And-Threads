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
#define MAX_REQUEST 4096

/*-----------------DATA STRUCTURE-------------*/

typedef struct
{
  int n_fields;
  int n_rows;
  char fields[MAX_FIELDS][MAX_NAME];
  char ***rows;
} DataBase;

typedef struct
{
  int client_socket;
  char command[MAX_REQUEST];
} Job;

struct node
{
  struct node *next;
  Job job;
};

typedef struct
{
  struct node *head;
  struct node *tail;
  int n;
  int requests_handled;
} RequestList;

/*-----------------GLOBALS--------------------*/
typedef struct
{
  int id;
  int active;
} ThreadData;

pthread_t *thread_ids; // Path calculator thread PIDS
ThreadData *td;
DataBase db;

RequestList RL;

sig_atomic_t exit_requested = 0; //SIGINT FLAG

pthread_mutex_t mutex_stdout = PTHREAD_MUTEX_INITIALIZER; // File lock mutex for stdout
pthread_cond_t cond_job = PTHREAD_COND_INITIALIZER;       // Signal new jobs
pthread_mutex_t mutex_job = PTHREAD_MUTEX_INITIALIZER;    // Thread data mutex (job)

int server_socket, // Server descriptor
    client_socket, // Client descriptor
    n_running;     // Currently busy threads

int opt_P, _P, // Input port no
    opt_O,
    opt_L, _L, // Pool size
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

// Job queue
void add_request(int client_socket, char request[MAX_REQUEST]);
Job get_next_request(void);

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

  print_log("Loading dataset...");
  clock_t t = clock();

  DataBase *db = db_init();

  t = clock() - t;
  double time_taken = ((double)t) / CLOCKS_PER_SEC; // calculate the elapsed time

  print_log("Dataset loaded in %f seconds with %d records", time_taken, db->n_rows);

  db_print(db, 0, 5);

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

  print_log("A pool of %d threads has been created.", _L);
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
      char buffer[MAX_REQUEST];

      while (read(client_socket, buffer, sizeof(buffer)))
      {
        printf("REQUEST: %s\n", buffer);

        // write(client_socket, "CTRL+C Mühendisi", strlen("CTRL+C Mühendisi"));
        // Add request to jobs
        add_request(client_socket, buffer);
        pthread_cond_signal(&cond_job); // Signal new job
      }
    }
  }

  /*--------------Free resources--------------------------*/
  print_log("Termination signal received, waiting for ongoing threads to complete.");
  
  exit_requested = 1;
  pthread_cond_broadcast(&cond_job);

  // Join threads
  // ...
  for (int i = 0; i < _L; i++)
    pthread_join(thread_ids[i], NULL);

  print_log("All threads have terminated, server shutting down.");

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
  ThreadData *td = data;
  print_log("Thread #%d: waiting for connection.", td->id);

  while (!exit_requested)
  {
    pthread_mutex_lock(&mutex_job); //mutex lock
    while (RL.n == 0){
      print_log("Thread #%d: waiting for job.", td->id);
      pthread_cond_wait(&cond_job, &mutex_job); //wait for the condition
      if (exit_requested){
        print_log("Thread #%d: exitting.", td->id);
        pthread_mutex_unlock(&mutex_job);
        return NULL;
      } 
    }

    print_log("Thread-%d is handling request", td->id);
    Job curr_job;
    curr_job = get_next_request();

    pthread_mutex_unlock(&mutex_job);

    // Do the deed

    write(curr_job.client_socket, "CTRL+C Mühendisi", strlen("CTRL+C Mühendisi"));
    print_log("Thread-%d handled request", td->id);

    RL.requests_handled++;
  }

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

void add_request(int client_socket, char request[MAX_REQUEST])
{
  struct node *newnode = malloc(sizeof(struct node));
  newnode->job.client_socket = client_socket;
  newnode->next = NULL;
  if (RL.tail == NULL)
  {
    RL.head = newnode;
  }
  else
  {
    RL.tail->next = newnode;
  }

  RL.tail = newnode;
  strcpy(RL.tail->job.command, request);
  RL.n++;
}

Job get_next_request(void)
{
  if (RL.head == NULL)
  {
    Job result = {0};
    pthread_mutex_unlock(&mutex_job);
    return result;
  }

  else
  {
    Job result = RL.head->job;
    struct node *temp = RL.head;
    RL.head = RL.head->next;
    if (RL.head == NULL)
    {
      RL.tail = NULL;
    }
    free(temp);
    RL.n--;
    return result;
  }
}