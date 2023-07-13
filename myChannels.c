#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

// Define the structure to hold channel information
typedef struct {
    char* file_path;
    float alpha;
    float beta;
    char* file;
    int start; 
    int end;
    int* samples;
    float* sample_value_beta;
    int samples_length;
} ChannelInfo;

// Global variables
int buffer_size;
int num_threads;
char* metadata_file_path;
int lock_config;
int global_checkpointing;
char* output_file_path;
int turn = 0;
int threads_working;

ChannelInfo* channels;
int num_channels;
float* samples;
int num_samples;

pthread_mutex_t global_lock;
pthread_mutex_t* granular_locks;
pthread_cond_t condition;
int num_finished_threads;

int read_file_content(int channel_index){
    // printf("Reading file: %s\n", channels[channel_index].file_path);
    FILE* channel_file = fopen(channels[channel_index].file_path, "rb");
    if (channel_file == NULL) {
        printf("Error opening channel file: %s\n", channels[channel_index].file_path);
        return -1;
    }
    // Allocate a buffer to hold the first three characters
    char buffer[1] = {0}; // initializing to zero
    // Read the first three characters into the buffer
    fread(buffer, 1, 1, channel_file);
    long file_size;
    // Convert buffer to integer and check if it is valid integer
    if(buffer[0] >= '0' && buffer[0] <='9'){
        fseek(channel_file, 0, SEEK_END);
        file_size = ftell(channel_file);
        fseek(channel_file, 0, SEEK_SET);
    }else if(buffer[0] == 0){
        printf("Empty channel file: %s\n", channels[channel_index].file_path);
        return -1;
    }else{
        fseek(channel_file, 0, SEEK_END);
        file_size = ftell(channel_file) - 3;
        // Seek to the 4th cha of the file
        fseek(channel_file, 3, SEEK_SET);
    }
    // Allocate a buffer to hold the entire file
    channels[channel_index].file = malloc((file_size + 1) * sizeof(char));
    // Read the entire file into the buffer
    fread(channels[channel_index].file, 1, file_size, channel_file);
    // Null-terminate the string
    channels[channel_index].file[file_size] = '\0';
    fclose(channel_file);
    return 0;
}

// Function to read the metadata file and populate the channel information
void readMetadataFile() {
    FILE* metadata_file = fopen(metadata_file_path, "r");
    if (metadata_file == NULL) {
        printf("Error opening metadata file\n");
        exit(1);
    }
    char buffer[1] = {0}; // initializing to zero
    fread(buffer, 1, 1, metadata_file);
    if(buffer[0] >= '0' && buffer[0] <='9'){
        fseek(metadata_file, 0, SEEK_END);
        fseek(metadata_file, 0, SEEK_SET);
    }else{
        fseek(metadata_file, 0, SEEK_END);
        fseek(metadata_file, 3, SEEK_SET);
    }
    // fgets(first_line, 10, metadata_file);
    fscanf(metadata_file, "%d", &num_channels);
    // printf("Number of channels: %d\n", num_channels);

    channels = malloc(num_channels * sizeof(ChannelInfo));

    char file_path[100];
    fpos_t position;
    for (int i = 0; i < num_channels; i++) {
        fscanf(metadata_file, "%s", file_path);

        channels[i].file_path = malloc((strlen(file_path) + 1) * sizeof(char));
        strcpy(channels[i].file_path, file_path);

        // Save the current position of the file pointer
        fgetpos(metadata_file, &position);

        if(fscanf(metadata_file, "%f", &channels[i].alpha) != 1){
            // If the next token is not a float, then the previous token was the file path
            // Reset the file pointer to the previous position
            fsetpos(metadata_file, &position);
            channels[i].alpha = 1;
            channels[i].beta = 1;
        }else{
            fgetpos(metadata_file, &position);
            if(fscanf(metadata_file, "%f", &channels[i].beta) != 1){
                // If the next token is not a float, then the previous token was the file path
                // Reset the file pointer to the previous position
                fsetpos(metadata_file, &position);
                channels[i].beta = 1;
            }
        }
        channels[i].start = 0;
        channels[i].end = buffer_size - 1;
        channels[i].samples = malloc(sizeof(int));
        channels[i].samples_length = 0;
        if(read_file_content(i) == -1){
            for(int j=0; j <= i; j++){
                free(channels[j].file_path);
                free(channels[j].file);
                free(channels[j].samples);
            }
            free(channels);
            exit(1);
        };
        // printf("Channel %d: %s, %f, %f\n", i, channels[i].file_path, channels[i].alpha, channels[i].beta);
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
    // Handle integer overflow
    if (new_sample > 65535) {
        new_sample = 65535;
    }
    return new_sample;
}

void process_sample_value(int i){
    channels[i].sample_value_beta = malloc(num_samples * sizeof(float));
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

        channels[i].sample_value_beta[j] = processSample_beta(sample_alpha, channels[i].beta);
        // printf("alpha = %f, beta = %f\n",sample_alpha, channels[i].sample_value_beta[j]);
    }
}

// Read k bytes from channelFile
int processChannelFile(int i) {
    int only_return = 1;
    int file_size = strlen(channels[i].file);
    if(channels[i].start >= file_size) return -1;

    // If there's a '\r' in the buffer, move the end index one position to the right
    char sample_value[buffer_size];
    int j = 0;  // index in sample_value
    for (int k = channels[i].start; k <= channels[i].end; k++) {
        if (channels[i].file[k] == '\r') {
            channels[i].end++;
        } else if(channels[i].file[k] >= '0' && channels[i].file[k] <= '9'){
            only_return = 0;
            sample_value[j++] = channels[i].file[k];
            sample_value[j] = '\0';  // ensure null termination
        } else if (channels[i].file[k] == '\n') {
            if (only_return == 0) {
                continue;
            }else if(only_return == 1){
                sample_value[j] = 0;
            }
        }
    }
    if(sample_value != NULL){
        //extend channels[i].samples length and add sample_value to it.
        channels[i].samples = realloc(channels[i].samples, (channels[i].samples_length + 1) * sizeof(int));

        channels[i].samples[channels[i].samples_length] = atoi(sample_value);
        printf("                                %d  pos: %d\n", channels[i].samples[channels[i].samples_length],channels[i].samples_length);
        channels[i].samples_length += 1;
    }
    // Update the start and end indices
    channels[i].start = channels[i].end + 1;
    channels[i].end = channels[i].start + buffer_size - 1;
    if (channels[i].end >= file_size) {
        channels[i].end = file_size - 1;
    }
    // Update the global num_samples variable
    if(num_samples < channels[i].samples_length){
        num_samples = channels[i].samples_length;
        granular_locks = realloc(granular_locks, num_samples * sizeof(pthread_mutex_t));
        // Initialize the new locks
        for (int i = num_samples; i < num_samples; i++) {
            pthread_mutex_init(&granular_locks[i], NULL);
        }
    }
    return 1;
}

int cas_float(float *ptr, float oldval, float newval) {
    uint32_t oldval_bits = *(uint32_t*)&oldval;
    uint32_t newval_bits = *(uint32_t*)&newval;
    return __sync_bool_compare_and_swap((uint32_t*)ptr, oldval_bits, newval_bits);
}

void process_ouput_value(int channel_index){
    if(lock_config == 1){
        pthread_mutex_lock(&global_lock);
        for(int j = 0; j < num_samples; j++){
            samples[j] += channels[channel_index].sample_value_beta[j];
        }
        pthread_mutex_unlock(&global_lock);
    }else if(lock_config == 2){
        for(int j = 0; j < num_samples; j++){
            pthread_mutex_lock(&granular_locks[j]);
            samples[j] += channels[channel_index].sample_value_beta[j];
            pthread_mutex_unlock(&granular_locks[j]);
        }
    }else if(lock_config == 3){
        for(int j = 0; j < num_samples; j++){
            for(int j = 0; j < num_samples; j++){
                float old_sample_value, new_sample_value;
                do {
                    old_sample_value = samples[j];
                    new_sample_value = old_sample_value + channels[channel_index].sample_value_beta[j];
                } while (!cas_float(&samples[j], old_sample_value, new_sample_value));
            }
        }
    }
}

// Function executed by each thread
void* threadFunction(void* arg) {
    int thread_id = *((int*)arg);
    int files_per_thread = num_channels / num_threads;
    int start_channel = thread_id * files_per_thread;
    int end_channel = start_channel + files_per_thread - 1;
    int count = 0;
    int i = start_channel;
    
    // Process the assigned channel files
    while(1){
        if(threads_working == 0) break;
        if(i > end_channel) i = start_channel;
        if(global_checkpointing){
            pthread_mutex_lock(&global_lock);
            if(turn != thread_id){
                pthread_cond_wait(&condition, &global_lock);
            }
        }
        printf("thread %d processing channel %d\n", thread_id, i);
        int res = processChannelFile(i);
        if(global_checkpointing){
            turn = (turn + 1) % num_threads;
            pthread_cond_broadcast(&condition);
            pthread_mutex_unlock(&global_lock);
        }
        // sleep(0.01);
        if(res == -1){
            count++;
            if(count == files_per_thread){
                if(global_checkpointing){
                    threads_working--; // global_checkpointing == 1 mode
                }else{
                    break; // global_checkpointing == 0 mode
                }
            }
            i++;
            continue;
        }
        
        if(res != -1 && count > 0){
            count = 0;
        }
        i++;
    }

    for(int j = start_channel; j <= end_channel; j++){
        process_sample_value(j);
        process_ouput_value(j);
    }

    return NULL;
}

// Function to write the final output to the output file
void writeOutputFile() {
    FILE* output_file = fopen(output_file_path, "w");

    for (int i = 0; i < num_samples; i++) {
        // Round up to the nearest integer
        int rounded_value = (int)samples[i];
        if (samples[i] - rounded_value > 0.0) {
            rounded_value++;
        }
        if(i == num_samples - 1){fprintf(output_file, "%d", rounded_value); break;}
        fprintf(output_file, "%d\n", rounded_value);
    }

    fclose(output_file);
}

int main(int argc, char* argv[]) {

    if (argc != 7) {
        printf("Invalid number of arguments\n");
        exit(1);
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
    if (num_channels % num_threads != 0) {
        printf("file is not in multiple of threads.\n");
        for(int i = 0; i < num_channels; i++){
            free(channels[i].samples);
            free(channels[i].file);
            free(channels[i].file_path);
        }
        free(channels);
        exit(1);
    }

    // Initialize locks and condition variable
    pthread_mutex_init(&global_lock, NULL);
    granular_locks = malloc(num_samples * sizeof(pthread_mutex_t));
    for (int i = 0; i < num_samples; i++) {
        pthread_mutex_init(&granular_locks[i], NULL);
    }
    pthread_cond_init(&condition, NULL);

    // Initialize arrays
    samples = calloc(num_samples, sizeof(float));

    // Create threads
    pthread_t* threads = malloc(num_threads * sizeof(pthread_t));
    int* thread_ids = malloc(num_threads * sizeof(int));
    threads_working = num_threads;

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
        
        free(channels[i].file_path);
        free(channels[i].file);
        free(channels[i].samples);
        free(channels[i].sample_value_beta);
    }
    for(int i = 0; i < num_samples; i++){
        pthread_mutex_destroy(&granular_locks[i]);
    }
    free(granular_locks);
    free(channels);
    free(samples);
    free(threads);
    free(thread_ids);

    return 0;
}
