#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    int buffer_size;
    int count;
    int next_value;
    int upper_limit;
    int buffer[1];
} SharedData;

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <consumer_id>\n", argv[0]);
        return 1;
    }

    int id = atoi(argv[1]);

    int fd = shm_open("/producer_consumer_shm", O_RDWR, 0666);
    if (fd == -1) {
        perror("shm_open");
        return 1;
    }

    struct stat st;
    if (fstat(fd, &st) == -1) {
        perror("fstat");
        return 1;
    }

    SharedData *shared = mmap(NULL, st.st_size,
                              PROT_READ | PROT_WRITE,
                              MAP_SHARED, fd, 0);

    if (shared == MAP_FAILED) {
        perror("mmap");
        return 1;
    }

    while (1) {
        if (shared->count > 0) {
            int index = (shared->count - 1) % shared->buffer_size;
            int value = shared->buffer[index];

            shared->count--;

            printf("Consumer %d consumed: %d\n", id, value);
            fflush(stdout);
        } else if (shared->next_value > shared->upper_limit) {
            break;
        }
    }

    munmap(shared, st.st_size);
    close(fd);
    return 0;
}
