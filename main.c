#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

#define SHM_NAME "/producer_consumer_shm"

// Shared region: only state that several processes must see lives here.
// buffer_size and upper_limit are NOT shared. Each child receives them
// as command-line arguments and keeps them in local variables.
typedef struct {
    int count;       // items believed to be in the buffer
    int in;          // next slot to write (shared by producers)
    int out;         // next slot to read (shared by consumers)
    int next_value;  // next value to produce (shared by producers)
    int buffer[1];   // bounded buffer, really buffer_size ints
} SharedData;

void print_usage(const char *program) {
    fprintf(stderr,
            "Usage: %s <buffer_size> <num_producers> "
            "<num_consumers> <upper_limit>\n"
            "  buffer_size, num_producers, num_consumers: positive integers\n"
            "  upper_limit: integer >= 0\n",
            program);
}

// Parse a whole string as an int. Returns 1 on success, 0 on failure.
int parse_int(const char *text, int *result) {
    char *end;
    errno = 0;
    long value = strtol(text, &end, 10);

    if (errno != 0 || end == text || *end != '\0') {
        return 0;
    }
    if (value < INT_MIN || value > INT_MAX) {
        return 0;
    }

    *result = (int)value;
    return 1;
}

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
        print_usage(argv[0]);
        return 1;
    }

    int buffer_size;
    int num_producers;
    int num_consumers;
    int upper_limit;

    if (!parse_int(argv[1], &buffer_size) ||
        !parse_int(argv[2], &num_producers) ||
        !parse_int(argv[3], &num_consumers) ||
        !parse_int(argv[4], &upper_limit)) {
        fprintf(stderr, "Error: all arguments must be integers.\n");
        print_usage(argv[0]);
        return 1;
    }

    if (buffer_size <= 0 || num_producers <= 0 ||
        num_consumers <= 0 || upper_limit < 0) {
        fprintf(stderr, "Error: invalid argument value.\n");
        print_usage(argv[0]);
        return 1;
    }

    size_t memory_size =
        sizeof(SharedData) + (buffer_size - 1) * sizeof(int);

    // Remove any leftover segment from a run that was interrupted.
    shm_unlink(SHM_NAME);

    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);

    if (fd == -1) {
        perror("shm_open");
        return 1;
    }

    if (ftruncate(fd, memory_size) == -1) {
        perror("ftruncate");
        close(fd);
        shm_unlink(SHM_NAME);
        return 1;
    }

    SharedData *shared = mmap(NULL, memory_size,
                              PROT_READ | PROT_WRITE,
                              MAP_SHARED, fd, 0);

    if (shared == MAP_FAILED) {
        perror("mmap");
        close(fd);
        shm_unlink(SHM_NAME);
        return 1;
    }

    shared->count = 0;
    shared->in = 0;
    shared->out = 0;
    shared->next_value = 0;

    // Process-local configuration passed to every child through exec.
    char size_arg[16];
    char limit_arg[16];
    snprintf(size_arg, sizeof(size_arg), "%d", buffer_size);
    snprintf(limit_arg, sizeof(limit_arg), "%d", upper_limit);

    int total_children = num_producers + num_consumers;
    double children_cpu_time = 0.0;

    struct timespec start;
    struct timespec end;
    struct rusage main_start;
    struct rusage main_end;

    // Begin measurement immediately before launching processes.
    clock_gettime(CLOCK_MONOTONIC, &start);
    getrusage(RUSAGE_SELF, &main_start);

    // Launch producers.
    for (int i = 0; i < num_producers; i++) {
        pid_t pid = fork();

        if (pid == 0) {
            char id[16];
            snprintf(id, sizeof(id), "%d", i + 1);
            execl("./producer", "producer", id, size_arg, limit_arg,
                  (char *)NULL);
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
            execl("./consumer", "consumer", id, size_arg, limit_arg,
                  (char *)NULL);
            perror("execl consumer");
            exit(1);
        } else if (pid < 0) {
            perror("fork consumer");
            return 1;
        }
    }

    // Reap every child and collect its CPU usage exactly once.
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

    // Measurements go to stderr so stdout can be redirected to a file.
    fprintf(stderr, "\n--- Performance Measurements ---\n");
    fprintf(stderr, "Elapsed time: %.6f seconds\n", elapsed);
    fprintf(stderr, "Total CPU time: %.6f seconds\n", total_cpu_time);
    fprintf(stderr, "Logical CPU cores: %ld\n", logical_cores);
    fprintf(stderr, "Average CPU utilization: %.2f%%\n",
            cpu_utilization);

    munmap(shared, memory_size);
    close(fd);
    shm_unlink(SHM_NAME);

    return 0;
}
