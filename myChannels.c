#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

// Define the structure to hold channel information
typedef struct {
    char* file_path;
    float alpha;
    float beta;
} ChannelInfo;

// Global variables
int buffer_size;
int num_threads;
char* metadata_file_path;
int lock_config;
int global_checkpointing;
char* output_file_path;

ChannelInfo* channels;
int num_channels;
int* samples;
int num_samples;
int* processed_samples;

pthread_mutex_t global_lock;
pthread_mutex_t* granular_locks;
pthread_cond_t condition;
int num_finished_threads;

// Function to read the metadata file and populate the channel information
void readMetadataFile() {
    FILE* metadata_file = fopen(metadata_file_path, "r");
    if (metadata_file == NULL) {
        printf("Error opening metadata file\n");
        exit(1);
    }

    fseek(metadata_file, 3, SEEK_SET);

    // fgets(first_line, 10, metadata_file);
    fscanf(metadata_file, "%d", &num_channels);
    printf("Number of channels: %d\n", num_channels);

    channels = malloc(num_channels * sizeof(ChannelInfo));

    char file_path[100];

    // Skip the newline characters after num_channels

    for (int i = 0; i < num_channels; i++) {
        fscanf(metadata_file, "%s", file_path);
        // Skip the newline characters after the file path

        channels[i].file_path = malloc((strlen(file_path) + 1) * sizeof(char));
        strcpy(channels[i].file_path, file_path);

        fscanf(metadata_file, "%f", &channels[i].alpha);
        fscanf(metadata_file, "%f", &channels[i].beta);
        printf("Channel %d: %s, %f, %f\n", i, channels[i].file_path, channels[i].alpha, channels[i].beta);
    }

    fclose(metadata_file);
}



// Function to apply low-pass filter and amplification to a sample
int processSample(int sample, float alpha, float beta, int previous_sample) {
    float new_sample_value = alpha * sample + (1 - alpha) * previous_sample;
    new_sample_value *= beta;
    
    // Round up to the nearest integer
    int rounded_value = (int)new_sample_value;
    if (new_sample_value - rounded_value > 0.0) {
        rounded_value++;
    }

    // Handle integer overflow
    if (rounded_value > 65535) {
        rounded_value = 65535;
    }

    return rounded_value;
}

// Function to read a channel file and process the samples
void processChannelFile(int channel_index) {
    FILE* channel_file = fopen(channels[channel_index].file_path, "r");
    if (channel_file == NULL) {
        printf("Error opening channel file: %s\n", channels[channel_index].file_path);
        exit(1);
    }

    int sample;
    int previous_sample = 0;

    while (fscanf(channel_file, "%d", &sample) != EOF) {
        // Apply low-pass filter and amplification to the sample
        int processed_sample = processSample(sample, channels[channel_index].alpha, channels[channel_index].beta, previous_sample);

        // Store the processed sample in the output channel
        pthread_mutex_lock(&granular_locks[channel_index]);
        samples[channel_index] += processed_sample;
        pthread_mutex_unlock(&granular_locks[channel_index]);

        previous_sample = processed_sample;
    }

    fclose(channel_file);
}

// Function executed by each thread
void* threadFunction(void* arg) {
    int thread_id = *((int*)arg);
    int files_per_thread = num_channels / num_threads;
    int start_channel = thread_id * files_per_thread;
    int end_channel = start_channel + files_per_thread;

    // Process the assigned channel files
    for (int i = start_channel; i < end_channel; i++) {
        processChannelFile(i);
    }

    // Synchronization
    if (global_checkpointing) {
        pthread_mutex_lock(&global_lock);
        num_finished_threads++;
        if (num_finished_threads == num_threads) {
            pthread_cond_broadcast(&condition);
        } else {
            while (num_finished_threads < num_threads) {
                pthread_cond_wait(&condition, &global_lock);
            }
        }
        pthread_mutex_unlock(&global_lock);
    } else {
        for (int i = start_channel; i < end_channel; i++) {
            pthread_mutex_lock(&granular_locks[i]);
            processed_samples[i]++;
            if (processed_samples[i] == num_threads) {
                pthread_cond_broadcast(&condition);
            } else {
                while (processed_samples[i] < num_threads) {
                    pthread_cond_wait(&condition, &granular_locks[i]);
                }
            }
            pthread_mutex_unlock(&granular_locks[i]);
        }
    }

    return NULL;
}

// Function to write the final output to the output file
void writeOutputFile() {
    FILE* output_file = fopen(output_file_path, "w");
    if (output_file == NULL) {
        printf("Error opening output file\n");
        exit(1);
    }

    for (int i = 0; i < num_samples; i++) {
        fprintf(output_file, "%d\n", samples[i]);
    }

    fclose(output_file);
}

int main(int argc, char* argv[]) {

    if (argc != 7) {
        printf("Invalid number of arguments\n");
        return 1;
    }

    // Parse command-line arguments
    buffer_size = atoi(argv[1]);
    num_threads = atoi(argv[2]);
    metadata_file_path = argv[3];
    lock_config = atoi(argv[4]);
    global_checkpointing = atoi(argv[5]);
    output_file_path = argv[6];

    // Initialize locks and condition variable
    // pthread_mutex_init(&global_lock, NULL);
    // granular_locks = malloc(num_channels * sizeof(pthread_mutex_t));
    // for (int i = 0; i < num_channels; i++) {
    //     pthread_mutex_init(&granular_locks[i], NULL);
    // }
    // pthread_cond_init(&condition, NULL);

    // Read the metadata file
    readMetadataFile();

    // Calculate the total number of samples
    num_samples = 0;
    for (int i = 0; i < num_channels; i++) {
        FILE* channel_file = fopen(channels[i].file_path, "r");
        if (channel_file == NULL) {
            printf("Error opening channel file: %s\n", channels[i].file_path);
            exit(1);
        }
        int sample;
        while (fscanf(channel_file, "%d", &sample) != EOF) {
            num_samples++;
        }
        fclose(channel_file);
    }

    // Initialize arrays
    samples = calloc(num_samples, sizeof(int));
    processed_samples = calloc(num_channels, sizeof(int));

    // Create threads
    pthread_t* threads = malloc(num_threads * sizeof(pthread_t));
    int* thread_ids = malloc(num_threads * sizeof(int));

    for (int i = 0; i < num_threads; i++) {
        thread_ids[i] = i;
        pthread_create(&threads[i], NULL, threadFunction, &thread_ids[i]);
    }

    // Wait for all threads to finish
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Write the final output
    writeOutputFile();

    // Clean up
    pthread_mutex_destroy(&global_lock);
    for (int i = 0; i < num_channels; i++) {
        pthread_mutex_destroy(&granular_locks[i]);
    }
    free(granular_locks);
    free(threads);
    free(thread_ids);
    for (int i = 0; i < num_channels; i++) {
        free(channels[i].file_path);
    }
    free(channels);
    free(samples);
    free(processed_samples);

    return 0;
}
