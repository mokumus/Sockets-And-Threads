#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h> // getopt(), access(), read(), close()
#include <arpa/inet.h>
#include <time.h> // time()

/* -------------------MACROS--------------------*/

// Exitting macro with a message
#define errExit(msg)    \
  do                    \
  {                     \
    perror(msg);        \
    exit(EXIT_FAILURE); \
  } while (0)
// Logging macro to add timestamp before prints
#define print_log(f_, ...) printf("%s ", timestamp()), printf((f_), ##__VA_ARGS__), printf("\n")

/* -------------------PROTOTYPES--------------------*/

// Misc functions
void print_usage();
char *timestamp();

int main(int argc, char *argv[])
{

  exit(EXIT_SUCCESS);
}

void print_usage(void)
{
  printf("\n========================================\n");
  printf("Usage:\n"
         "./client [-i id] [-a IP number] [-p PORT]  [-o path to query file]\n");
  printf("========================================\n");
}

char *timestamp()
{
  time_t now = time(NULL);
  char *time = asctime(gmtime(&now));
  time[strlen(time) - 1] = '\0'; // Remove \n
  return time;
}