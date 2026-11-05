#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define SHM_NAME "/producer_consumer_shm"

// Must match the struct in main.c exactly.
typedef struct {
    int count;
    int in;
    int out;
    int next_value;
    int buffer[1];
} SharedData;

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr,
                "Usage: %s <producer_id> <buffer_size> <upper_limit>\n",
                argv[0]);
        return 1;
    }

    // Process-local configuration: never changes, so no need to share it.
    int id = atoi(argv[1]);
    int buffer_size = atoi(argv[2]);
    int upper_limit = atoi(argv[3]);

    int fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (fd == -1) {
        perror("shm_open");
        return 1;
    }

    struct stat st;
    if (fstat(fd, &st) == -1) {
        perror("fstat");
        return 1;
    }

    // volatile: force a real memory read every time, because other
    // processes change these values. This is not synchronization.
    volatile SharedData *shared = mmap(NULL, st.st_size,
                                       PROT_READ | PROT_WRITE,
                                       MAP_SHARED, fd, 0);

    if (shared == MAP_FAILED) {
        perror("mmap");
        return 1;
    }

    while (1) {
        // Termination: stop once the shared counter passes the limit.
        if (shared->next_value > upper_limit) {
            break;
        }

        // Buffer appears not full.
        if (shared->count < buffer_size) {
            // Not atomic: another producer can read the same value here.
            int value = shared->next_value;
            shared->next_value = value + 1;

            int slot = shared->in;
            shared->buffer[slot] = value;
            shared->in = (slot + 1) % buffer_size;
            shared->count++;

            printf("Producer %d produced: %d\n", id, value);
            fflush(stdout);
        }
    }

    munmap((void *)shared, st.st_size);
    close(fd);
    return 0;
}
