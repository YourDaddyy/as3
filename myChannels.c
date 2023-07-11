#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

// Define the structure to hold channel information
typedef struct {
    char* file_path;
    float alpha;
    float beta;
    char* file;
    int* samples;
    float* samples_beta;
    int samples_length;
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
float processSample_alpha(int sample, float alpha, float previous_sample) {
    if(sample == previous_sample) return sample;

    float new_sample_value = alpha * sample + (1 - alpha) * previous_sample;

    // Handle integer overflow
    if (new_sample_value > 65535) {
        new_sample_value = 65535;
    }

    return new_sample_value;
}

// Function to apply low-pass filter and amplification to a sample
float processSample_beta(float sample, float beta) {
    float new_sample = sample * beta;
    
    // Round up to the nearest integer
    // int rounded_value = (int)new_sample;
    // if (new_sample - rounded_value > 0.0) {
    //     rounded_value++;
    // }

    // Handle integer overflow
    if (new_sample > 65535) {
        new_sample = 65535;
    }

    return new_sample;
}

int processSample(int sample, float alpha, float beta, int previous_sample) {
    float new_sample = sample * beta;
    
    // Round up to the nearest integer
    int rounded_value = (int)new_sample;
    if (new_sample - rounded_value > 0.0) {
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
    num_samples = 0;
    int local_num_samples = 0;
    char* file_content = read_file_content(channels[channel_index].file_path);
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

void process_ouput_value(){
        for(int i = 0; i < num_samples; i++){
            float output_value = 0.0;
            for(int j = 0; j < num_channels; j++){
                output_value += channels[j].samples_beta[i];
            }
            // Round up to the nearest integer
            int rounded_value = (int)output_value;
            if (output_value - rounded_value > 0.0) {
                rounded_value++;
            }
            samples[i] = rounded_value;
        }
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

char* read_file_content(char* file_path){
    FILE* channel_file = fopen(file_path, "rb");
    if (channel_file == NULL) {
        printf("Error opening channel file: %s\n", file_path);
        exit(1);
    }
    // Seek to the end of the file
    fseek(channel_file, 0, SEEK_END);
    // Get the size of the file
    long file_size = ftell(channel_file);
    // Seek back to the start of the file
    fseek(channel_file, 3, SEEK_SET);

    // Allocate a buffer to hold the entire file
    char* file_content = malloc((file_size + 1) * sizeof(char));

    // Read the entire file into the buffer
    fread(file_content, 1, file_size, channel_file);
    
    int file_length = strlen(file_content);

    // Null-terminate the string
    file_content[file_length] = '\0';

    fclose(channel_file);

    return file_content;
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

    // Read the metadata file
    readMetadataFile();

    // Initialize locks and condition variable
    pthread_mutex_init(&global_lock, NULL);
    granular_locks = malloc(num_channels * sizeof(pthread_mutex_t));
    for (int i = 0; i < num_channels; i++) {
        pthread_mutex_init(&granular_locks[i], NULL);
    }
    pthread_cond_init(&condition, NULL);

    // Initialize arrays
    samples = calloc(num_samples, sizeof(int));
    process_ouput_value();
    processed_samples = calloc(num_channels, sizeof(int));

    if (num_channels % num_threads != 0) {
        printf("Error: num_channels is not a multiple of num_threads.\n");
        exit(1);
    }

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

    // Calculate the total number of samples
    num_samples = 0;
    for (int i = 0; i < num_channels; i++) {
        int local_num_samples = 0;
        char* file_content = read_file_content(channels[i].file_path);

        // Start and end indices of the chunk
        int start = 0;
        int end = buffer_size - 1;
        int file_size = strlen(file_content);
        channels[i].samples = malloc(sizeof(int));
        // Process the large buffer in chunks
        while (start < file_size){
            // If there's a '\r' in the buffer, move the end index one position to the right
            int only_return = 1;
            char sample_value[buffer_size];
            int j = 0;  // index in sample_value
            for (int i = start; i <= end; i++) {
                if (file_content[i] == '\r') {
                    end++;
                } else if(file_content[i] >= '0' && file_content[i] <= '9'){
                    only_return = 0;
                    sample_value[j++] = file_content[i];
                    sample_value[j] = '\0';  // ensure null termination
                } else if (file_content[i] == '\n') {
                    if (only_return == 0) {
                        continue;
                    }else if(only_return == 1){
                        sample_value[j] = 0;
                    }
                }
            }
            
            if(sample_value != NULL){
                channels[i].samples = realloc(channels[i].samples, (local_num_samples + 1) * sizeof(int));

                int sample_int = atoi(sample_value);

                channels[i].samples[local_num_samples] = sample_int;
                printf("%d\n", channels[i].samples[local_num_samples]);

                local_num_samples++;
            }
            
            only_return = 1;  // reset for next chunk

            // Move to the next chunk
            start = end + 1;
            end = start + buffer_size - 1;
            if (end >= file_size) {
                end = file_size - 1;
            }
        }
        channels[i].samples_length = local_num_samples;
        if(num_samples < local_num_samples){
            num_samples = local_num_samples;
        }
        free(file_content);
    }

    for(int i = 0; i < num_channels; i++){
        channels[i].samples_beta = malloc(num_samples * sizeof(float));
        float previous_sample = channels[i].samples[0];
        for(int j = 0; j < num_samples; j++){
            float sample_alpha;
            if(j > channels[i].samples_length - 1){
                channels[i].samples = realloc(channels[i].samples, (j+1) * sizeof(int));
                channels[i].samples[j] = 0;
                channels[i].samples_length = j;
                sample_alpha = 0;
            }else{
                sample_alpha = processSample_alpha(channels[i].samples[j], channels[i].alpha, previous_sample);
                previous_sample = sample_alpha;
            }

            channels[i].samples_beta[j] = processSample_beta(sample_alpha, channels[i].beta);
            printf("alpha = %f, beta = %f\n",sample_alpha, channels[i].samples_beta[j]);
        }
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
