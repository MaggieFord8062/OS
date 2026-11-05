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
                "Usage: %s <consumer_id> <buffer_size> <upper_limit>\n",
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
        // Buffer appears not empty.
        if (shared->count > 0) {
            int slot = shared->out;
            int value = shared->buffer[slot];
            shared->out = (slot + 1) % buffer_size;
            shared->count--;

            printf("Consumer %d consumed: %d\n", id, value);
            fflush(stdout);
        } else if (shared->next_value > upper_limit) {
            // Termination: buffer looks empty and producers are done.
            break;
        }
    }

    munmap((void *)shared, st.st_size);
    close(fd);
    return 0;
}
