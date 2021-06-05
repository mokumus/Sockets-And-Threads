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
#include <semaphore.h>

/* ----------------DEFINES--------------------*/

#define MAX_PATH 1024   // Max file path length
#define MAX_BACKLOG 128 // Max server queue for listen()
#define MAX_FIELDS 30   // Max number of fields csv can have
#define MAX_NAME 64     // Max string lenght of field names
#define MAX_LINE 1024   // Max string of a data row(combined string length of all fields)
#define MAX_REQUEST 500

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
DataBase *db;

RequestList RL;

sig_atomic_t exit_requested = 0; //SIGINT FLAG

pthread_mutex_t mutex_stdout = PTHREAD_MUTEX_INITIALIZER; // File lock mutex for stdout
pthread_cond_t cond_job = PTHREAD_COND_INITIALIZER;       // Signal new jobs
pthread_mutex_t mutex_job = PTHREAD_MUTEX_INITIALIZER;    // Thread data mutex (job)
pthread_mutex_t mutex_db = PTHREAD_MUTEX_INITIALIZER;     // Thread data mutex (job)

int _AR = 0; // number of active readers
int _AW = 0; // number of active writers
int _WR = 0; // number of waiting readers
int _WW = 0; // number of waiting writers
pthread_cond_t okToRead = PTHREAD_COND_INITIALIZER;
pthread_cond_t okToWrite = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex_lock = PTHREAD_MUTEX_INITIALIZER;

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
void db_print(int start, int end, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int key_count);
void db_print_row(int n, int socket_fd, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int key_count);
int process_cmd(char cmd[MAX_REQUEST], int socket_fd);
int get_field_index(char *field_name, char delim);
int gather_output_star(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd);
int gather_output_some(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd);
int gather_output_distinct(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd);
int gather_output_update(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd, int index_where, char *where);
int gather(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd, int index_where, char *where, int mode);
void db_print_fields(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd);

int jenkins_one_at_a_time_hash(char *key);
int linear_search(int *arr, int n, int x);

// Job queue
void add_request(int client_socket);
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

  db = db_init();

  t = clock() - t;
  double time_taken = ((double)t) / CLOCKS_PER_SEC; // calculate the elapsed time

  print_log("Dataset loaded in %f seconds with %d records", time_taken, db->n_rows);

  //process_cmd("1 SELECT Age, Name FROM TABLE;", 0);

  //process_cmd("2 SELECT * FROM TABLE WHERE Age='24';", 0);

  //process_cmd("1 SELECT  Period Subject FROM TABLE;", 0);

  //process_cmd("2 UPDATE TABLE SET Series_title_5='This is a new title' WHERE Group='Industry by financial variable';", 0);

  //process_cmd("1 SELECT DISTINCT Series_reference FROM TABLE;", 0);

  //db_print(db, 0, 5);

  // Free DB

  /*
  for (int i = 0; i < db->n_rows; i++)
  {
    for (int k = 0; k < db->n_fields; k++)
      free(db->rows[i][k]);
    free(db->rows[i]);
  }
  free(db->rows);

  free(thread_ids);
  free(td);

  exit(EXIT_SUCCESS);
  */

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

      // write(client_socket, "CTRL+C Mühendisi", strlen("CTRL+C Mühendisi"));
      // Add request to jobs
      pthread_mutex_lock(&mutex_job);
      add_request(client_socket);

      pthread_mutex_unlock(&mutex_job);
      pthread_cond_signal(&cond_job); // Signal new job
    }
  }

  /*--------------Free resources--------------------------*/
  printf("\n");
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
    while (RL.n == 0)
    {
      //print_log("Thread #%d: waiting for job.", td->id);
      pthread_cond_wait(&cond_job, &mutex_job); //wait for the condition
      if (exit_requested)
      {
        print_log("Thread #%d: terminating.", td->id);
        pthread_mutex_unlock(&mutex_job);
        return NULL;
      }
    }

    print_log("A connection has been delegated to thread id #%d", td->id);
    Job curr_job;
    curr_job = get_next_request();
    //print_log("Thread #%d: received query ‘%s‘", td->id, curr_job.command);

    pthread_mutex_unlock(&mutex_job);

    char buffer[MAX_REQUEST];
    bzero(buffer, MAX_REQUEST);
    while (read(curr_job.client_socket, buffer, MAX_REQUEST) > 0)
    {
      int n_queries = 0;
      char id[32];
      char opt[32];
      sscanf(buffer, "%s %s", id, opt);

      print_log("Thread #%d: received query '%s'", td->id, buffer);
      usleep(500000); // Sleep 0.5 seconds

      if (opt[0] == 'U')
      {
        // Writer
        pthread_mutex_lock(&mutex_lock);
        while (_AW + _AR > 0)
        {
          _WW++;
          pthread_cond_wait(&okToWrite, &mutex_lock);
          _WW--;
        }
        _AW++;
        pthread_mutex_unlock(&mutex_lock);
        n_queries = process_cmd(buffer, curr_job.client_socket);
        pthread_mutex_lock(&mutex_lock);
        _AW--;
        if (_WW > 0)
          pthread_cond_signal(&okToWrite);
        else if (_WR > 0)
          pthread_cond_broadcast(&okToRead);
        pthread_mutex_unlock(&mutex_lock);
      }
      else
      {
        //Reader
        pthread_mutex_lock(&mutex_lock);
        while (_AW + _WW > 0)
        {
          _WR++;
          pthread_cond_wait(&okToRead, &mutex_lock);
          _WR--;
        }
        _AR++;
        pthread_mutex_unlock(&mutex_lock);
        n_queries = process_cmd(buffer, curr_job.client_socket);
        pthread_mutex_lock(&mutex_lock);
        _AR--;
        if (_AR == 0 && _WW > 0)
          pthread_cond_signal(&okToWrite);
        pthread_mutex_unlock(&mutex_lock);
      }

      write(curr_job.client_socket, "^", strlen("^") + 1);
      print_log("Thread #%d: query completed, %d records have been returned.", td->id, n_queries);
    }
    shutdown(curr_job.client_socket, SHUT_RDWR);
    close(curr_job.client_socket);
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
  token = strtok(line, ",\n\r");
  strcpy(db.fields[db.n_fields], token);
  while (token != NULL)
  {
    strcpy(db.fields[db.n_fields++], token);
    token = strtok(NULL, ",\n\r");
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

      if (line[i] == '\0' || line[i] == EOF || line[i] == '\n')
        continue;

      if (line[i + 1] == '\0' || line[i + 1] == EOF)
      {
        buffer[n] = 0;
        db.rows[curr_line][curr_field] = malloc((n + 1) * sizeof(char));
        strcpy(db.rows[curr_line][curr_field], buffer);
        continue;
      }

      if (line[i + 1] == '\n')
      {
        buffer[n] = 0;
        db.rows[curr_line][curr_field] = malloc((n + 1) * sizeof(char));
        strcpy(db.rows[curr_line][curr_field], buffer);
        n = 0;
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

void db_print(int start, int end, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int key_count)
{
  for (int i = start; i < end; i++)
    db_print_row(i, 0, field_indices, keys, key_count);
}

void db_print_row(int n, int socket_fd, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int key_count)
{
  if (socket_fd == 0)
  {
    if (keys == NULL)
    {
      printf("R%d:", n);
      for (int i = 0; i < db->n_fields; i++)
        printf("%s,", db->rows[n][i]);
      printf("\n");
    }
    else
    {
      printf("R%d:", n);
      for (int i = 0; i < key_count; i++)
      {
        //printf("%-15s == %-15s\n", db->fields[field_indices[i]], keys[i]);
        if (strcmp(db->fields[field_indices[i]], keys[i]) == 0)
          printf("%-17s", db->rows[n][field_indices[i]]);
      }

      printf("\n");
    }
  }
  else
  {
    char buffer[MAX_REQUEST];
    int j;
    char c;
    if (keys == NULL)
    {
      j = snprintf(buffer, MAX_REQUEST, "\n");
      //printf("j: %d\n", j);
      for (int i = 0; i < db->n_fields; i++)
        j += snprintf(&buffer[j], MAX_REQUEST, "%-17s ", db->rows[n][i]);

      write(socket_fd, buffer, strlen(buffer) + 1);
      read(socket_fd, &c, sizeof(char));
    }
    else
    {
      j = snprintf(buffer, MAX_REQUEST, "\n");
      for (int i = 0; i < key_count; i++)
      {
        if (strcmp(db->fields[field_indices[i]], keys[i]) == 0)
          j += snprintf(&buffer[j], MAX_REQUEST, "%-17s ", db->rows[n][field_indices[i]]);
      }
      write(socket_fd, buffer, strlen(buffer) + 1);
      read(socket_fd, &c, sizeof(char));
    }
  }
}

void add_request(int client_socket)
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
  RL.n++;
}

Job get_next_request(void)
{
  if (RL.head == NULL)
  {
    Job result = {0};
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

int process_cmd(char cmd[MAX_REQUEST], int socket_fd)
{

  char cmd_args[MAX_FIELDS][MAX_NAME];
  int curr_arg = 0;
  int in_quote = 0;
  int n = 0;
  int n_queries = 0;

  for (int i = 0; i < MAX_FIELDS; i++)
  {
    cmd_args[i][0] = '\0';
  }

  // Parse SQL Command into tokens and store it in cmd_args
  for (int i = 0; i < strlen(cmd); i++)
  {

    if (cmd[i + 1] == ';' || cmd[i + 1] == '\n' || cmd[i + 1] == 0)
    {
      if (cmd[i] != ',' && cmd[i] != '\'')
        cmd_args[curr_arg][n++] = cmd[i];
      cmd_args[curr_arg][n] = 0;
      break;
    }

    if (cmd[i] == '\'' || cmd[i] == '"')
    {

      in_quote = in_quote == 0 ? 1 : 0;
      continue;
    }

    if (!in_quote)
    {
      if (cmd[i] == ' ')
      {
        cmd_args[curr_arg++][n] = 0;
        n = 0;
      }

      else
      {
        if (cmd[i] != ',' && cmd[i] != '\'')
          cmd_args[curr_arg][n++] = cmd[i];
      }
    }
    else
    {
      cmd_args[curr_arg][n++] = cmd[i];
    }
  }

  // Process SQL command

  for (int i = 0; i <= curr_arg; i++)
  {
    //printf("%s + ", cmd_args[i]);
  }

  //printf("\n");
  //printf("curr_Arg=%d\n", curr_arg);

  if (strcmp(cmd_args[1], "SELECT") == 0)
  {
    if (strcmp(cmd_args[2], "*") == 0)
    {
      if (strcmp(cmd_args[5], "WHERE") == 0)
      {
        //printf("returning some db: ");

        int k = 0;
        int field_indices[MAX_FIELDS];
        char keys[MAX_FIELDS][MAX_LINE];
        for (int i = 6; i <= curr_arg; i++)
        {
          field_indices[k] = get_field_index(cmd_args[i], '=');
          strcpy(keys[k++], strchr(cmd_args[i], '=') + 1);
        }
        n_queries = gather(k, field_indices, keys, socket_fd, 0, NULL, 0);
      }
      else
      {
        n_queries = gather(0, NULL, NULL, socket_fd, 0, NULL, 0);
      }
    }
    else if (strcmp(cmd_args[2], "DISTINCT") == 0)
    {
      int k = 0;
      int field_indices[MAX_FIELDS];
      char keys[MAX_FIELDS][MAX_LINE];
      for (int i = 3; i <= curr_arg; i++)
      {
        if (strcmp(cmd_args[i], "FROM") == 0)
        {
          n_queries = gather(k, field_indices, keys, socket_fd, 0, NULL, 3);
          break;
        }
        field_indices[k] = get_field_index(cmd_args[i], '\0');
        strcpy(keys[k++], cmd_args[i]);
      }
    }
    else
    {
      int k = 0;
      int field_indices[MAX_FIELDS];
      char keys[MAX_FIELDS][MAX_LINE];
      for (int i = 2; i <= curr_arg; i++)
      {
        field_indices[k] = get_field_index(cmd_args[i], '\0');

        strcpy(keys[k++], cmd_args[i]);
        //printf("KEY{%d}: %s %d// ", k - 1, keys[k - 1], field_indices[k - 1]);
      }
      n_queries = gather(k, field_indices, keys, socket_fd, 0, NULL, 1);
    }
  }

  else
  {
    int i;
    int k = 0;
    int field_indices[MAX_FIELDS];
    char keys[MAX_FIELDS][MAX_LINE];
    char where[MAX_LINE];
    int index_where = 0;
    for (i = 4; i <= curr_arg; i++)
    {
      if (strcmp(cmd_args[i], "WHERE") == 0)
      {
        i++;
        index_where = get_field_index(cmd_args[i], '=');
        strcpy(where, strchr(cmd_args[i], '=') + 1);

        n_queries = gather(k, field_indices, keys, socket_fd, index_where, where, 2);

        break;
      }

      field_indices[k] = get_field_index(cmd_args[i], '=');
      strcpy(keys[k++], strchr(cmd_args[i], '=') + 1);

      //printf("%s + ", cmd_args[i]);
    }
    //printf("\n");
  }
  return n_queries;
}

int get_field_index(char *field_name, char delim)
{
  char temp_field[MAX_NAME];
  int ret = -1;
  strcpy(temp_field, field_name);
  if (delim != '\0')
  {

    char *p = strchr(temp_field, delim);
    if (!p)
      return ret;
    else
    {
      *p = 0;
    }
  }

  for (int i = 0; i < db->n_fields; i++)
  {
    if (strcmp(db->fields[i], temp_field) == 0)
      return i;
  }
  return ret;
}

int gather_output_star(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd)
{
  int n_records = 0;

  if (key_count > 0)
  {
    db_print_fields(key_count, field_indices, NULL, socket_fd);
    for (int k = 0; k < db->n_rows; k++)
    {
      int match = 0;
      for (int i = 0; i < key_count; i++)
      {
        //printf("%-15s == %-15s\n", db->rows[k][field_indices[i]], keys[i]);
        if (strcmp(db->rows[k][field_indices[i]], keys[i]) == 0)
        {
          match++;
        }

        if (match == key_count)
        {
          //printf("matches: %d\n", match);
          db_print_row(k, socket_fd, NULL, NULL, 0);
          n_records++;
          break;
        }
      }
    }
  }
  else
  {
    db_print_fields(key_count, field_indices, keys, socket_fd);
    // Select all
    n_records = db->n_rows;
    //printf("Found %d records\n", n_records);
    for (int k = 0; k < db->n_rows; k++)
      db_print_row(k, socket_fd, NULL, NULL, 0);
  }

  return n_records;
}

int gather_output_some(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd)
{
  int n_records = db->n_rows;
  db_print_fields(key_count, field_indices, keys, socket_fd);
  for (int k = 0; k < db->n_rows; k++)
    db_print_row(k, socket_fd, field_indices, keys, key_count);
  return n_records;
}

void db_print_fields(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd)
{
  int j;
  char c;
  char buffer[MAX_REQUEST];
  if (keys == NULL)
  {
    j = snprintf(buffer, MAX_REQUEST, "\n");
    for (int i = 0; i < db->n_fields; i++)
      j += snprintf(&buffer[j], MAX_REQUEST, "%-17s ", db->fields[i]);

    write(socket_fd, buffer, strlen(buffer) + 1);
    read(socket_fd, &c, sizeof(char));
  }
  else
  {
    j = snprintf(buffer, MAX_REQUEST, "\n");
    for (int i = 0; i < key_count; i++)
      j += snprintf(&buffer[j], MAX_REQUEST, "%-17s ", db->fields[field_indices[i]]);

    write(socket_fd, buffer, strlen(buffer) + 1);
    read(socket_fd, &c, sizeof(char));
  }
}

int gather_output_update(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd, int index_where, char *where)
{
  int n_records = 0;
  db_print_fields(key_count, field_indices, NULL, socket_fd);
  for (int i = 0; i < db->n_rows; i++)
  {
    if (strcmp(db->rows[i][index_where], where) == 0)
    {
      for (int j = 0; j < key_count; j++)
      {
        db->rows[i][field_indices[j]] = realloc(db->rows[i][field_indices[j]], strlen(keys[j]) + 1);
        strcpy(db->rows[i][field_indices[j]], keys[j]);
      }
      db_print_row(i, socket_fd, NULL, NULL, 0);
      n_records++;
    }
  }
  return n_records;
}

int gather(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd, int index_where, char *where, int mode)
{
  int ret;
  switch (mode)
  {
  case 0:
    // Star
    ret = gather_output_star(key_count, field_indices, keys, socket_fd);
    break;
  case 1:
    // Some
    ret = gather_output_some(key_count, field_indices, keys, socket_fd);
    break;
  case 2:
    // Update
    ret = gather_output_update(key_count, field_indices, keys, socket_fd, index_where, where);
    break;
  case 3:
    // Distinct
    ret = gather_output_distinct(key_count, field_indices, keys, socket_fd);
    break;

  default:
    break;
  }
  return ret;
}

int gather_output_distinct(int key_count, int field_indices[MAX_FIELDS], char keys[MAX_FIELDS][MAX_LINE], int socket_fd)
{
  int n_records = 0;
  int *hash_list = malloc(db->n_rows * sizeof(int));
  int n_hs = 0;
  memset(hash_list, -1, sizeof(int) * db->n_rows);
  db_print_fields(key_count, field_indices, keys, socket_fd);
  for (int i = 0; i < db->n_rows; i++)
  {
    char hash_str[MAX_LINE];
    int k = 0;

    // Build hash string
    //printf("key count: %d\n", key_count);
    for (int j = 0; j < key_count; j++)
      k += snprintf(&hash_str[k], MAX_LINE, "%s", db->rows[i][field_indices[j]]);

    
    int h = jenkins_one_at_a_time_hash(hash_str);

    //printf("hash string: %s %d\n", hash_str,h);
    if (linear_search(hash_list, n_hs, h) == -1)
    {
      hash_list[n_hs++] = h;
      n_records++;
      db_print_row(i, socket_fd, field_indices, keys, key_count);
    }
  }

  free(hash_list);
  return n_records;
}

int jenkins_one_at_a_time_hash(char *key)
{
  int hash, i;
  int len = strlen(key);
  for (hash = i = 0; i < len; ++i)
  {
    hash += key[i];
    hash += (hash << 10);
    hash ^= (hash >> 6);
  }
  hash += (hash << 3);
  hash ^= (hash >> 11);
  hash += (hash << 15);

  return hash;
}

int linear_search(int *arr, int n, int x)
{
  for (int i = 0; i < n; i++)
    if (arr[i] == x)
      return i;
    else if (arr[i] == -1)
      break;
  return -1;
}