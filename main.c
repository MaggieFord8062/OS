
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <time.h>
#include <errno.h>

typedef struct {
    int buffer_size;
    int count;
    int next_value;
    int upper_limit;
    int buffer[1];
} SharedData;

// Convert a timeval to seconds.
double timeval_to_seconds(struct timeval t) {
    return t.tv_sec + t.tv_usec / 1e6;
}

// Get total CPU time (user + system) from resource usage.
double cpu_time(struct rusage usage) {
    return timeval_to_seconds(usage.ru_utime) +
           timeval_to_seconds(usage.ru_stime);
}

int main(int argc, char *argv[]) {
    if (argc != 5) {
        fprintf(stderr,
                "Usage: %s <buffer_size> <num_producers> "
                "<num_consumers> <upper_limit>\n",
                argv[0]);
        return 1;
    }

    int buffer_size = atoi(argv[1]);
    int num_producers = atoi(argv[2]);
    int num_consumers = atoi(argv[3]);
    int upper_limit = atoi(argv[4]);

    if (buffer_size <= 0 || num_producers <= 0 ||
        num_consumers <= 0 || upper_limit < 0) {
        fprintf(stderr, "Invalid arguments.\n");
        return 1;
    }

    size_t memory_size =
        sizeof(SharedData) + (buffer_size - 1) * sizeof(int);

    int fd = shm_open("/producer_consumer_shm",
                      O_CREAT | O_RDWR, 0666);

    if (fd == -1) {
        perror("shm_open");
        return 1;
    }

    if (ftruncate(fd, memory_size) == -1) {
        perror("ftruncate");
        close(fd);
        shm_unlink("/producer_consumer_shm");
        return 1;
    }

    SharedData *shared = mmap(NULL, memory_size,
                              PROT_READ | PROT_WRITE,
                              MAP_SHARED, fd, 0);

    if (shared == MAP_FAILED) {
        perror("mmap");
        close(fd);
        shm_unlink("/producer_consumer_shm");
        return 1;
    }

    shared->buffer_size = buffer_size;
    shared->count = 0;
    shared->next_value = 0;
    shared->upper_limit = upper_limit;

    int total_children = num_producers + num_consumers;
    double children_cpu_time = 0.0;

    struct timespec start, end;
    struct rusage main_start, main_end;

    // Begin measurement immediately before launching processes.
    clock_gettime(CLOCK_MONOTONIC, &start);
    getrusage(RUSAGE_SELF, &main_start);

    // Launch producers.
    for (int i = 0; i < num_producers; i++) {
        pid_t pid = fork();

        if (pid == 0) {
            char id[16];
            snprintf(id, sizeof(id), "%d", i + 1);
            execl("./producer", "producer", id, (char *)NULL);
            perror("execl producer");
            exit(1);
        } else if (pid < 0) {
            perror("fork producer");
            return 1;
        }
    }

    // Launch consumers.
    for (int i = 0; i < num_consumers; i++) {
        pid_t pid = fork();

        if (pid == 0) {
            char id[16];
            snprintf(id, sizeof(id), "%d", i + 1);
            execl("./consumer", "consumer", id, (char *)NULL);
            perror("execl consumer");
            exit(1);
        } else if (pid < 0) {
            perror("fork consumer");
            return 1;
        }
    }

    // Collect each child's CPU usage exactly once.
    int completed = 0;

    while (completed < total_children) {
        struct rusage child_usage;
        int status;

        pid_t result = wait4(-1, &status, 0, &child_usage);

        if (result > 0) {
            children_cpu_time += cpu_time(child_usage);
            completed++;
        } else if (result == -1 && errno == EINTR) {
            continue;
        } else {
            perror("wait4");
            break;
        }
    }

    getrusage(RUSAGE_SELF, &main_end);
    clock_gettime(CLOCK_MONOTONIC, &end);

    double elapsed =
        (end.tv_sec - start.tv_sec) +
        (end.tv_nsec - start.tv_nsec) / 1e9;

    double main_cpu_time =
        cpu_time(main_end) - cpu_time(main_start);

    double total_cpu_time =
        children_cpu_time + main_cpu_time;

    long logical_cores = sysconf(_SC_NPROCESSORS_ONLN);

    if (logical_cores < 1) {
        logical_cores = 1;
    }

    double cpu_utilization =
        (total_cpu_time / (logical_cores * elapsed)) * 100.0;

    printf("\n--- Performance Measurements ---\n");
    printf("Execution time: %.6f seconds\n", elapsed);
    printf("Total CPU time: %.6f seconds\n", total_cpu_time);
    printf("Logical CPU cores: %ld\n", logical_cores);
    printf("Average CPU utilization: %.2f%%\n",
           cpu_utilization);

    munmap(shared, memory_size);
    close(fd);
    shm_unlink("/producer_consumer_shm");

    return 0;
}

