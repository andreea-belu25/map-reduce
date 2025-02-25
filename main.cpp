#include <iostream>
#include <fstream>
#include <pthread.h>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <sstream>

using namespace std;

struct map_data {
    vector<string>* files;
    vector<pair<string, int>> partial_lists;
    pthread_mutex_t mutex;
    pthread_barrier_t barrier;
};

struct map_thread {
    map_data* data;
    int thread_id;
};

struct reduce_data {
    int no_of_reducers;
    map_data* mapping_data;
    vector<string>* letters;
    unordered_map<string, unordered_map<string, vector<int>>> final_lists;
    pthread_mutex_t mutex;
    pthread_barrier_t barrier;
};

struct reduce_thread {
    map_data* map_data_for_reducing;
    reduce_data* reducing_data;
    int thread_id;
};

void write_words_to_file(string letter, vector<pair<string, vector<int>>> words) {
    string output_file = letter + ".txt";
    
    ofstream fout(output_file);
    if (!fout.is_open()) {
        cout << "Error: something bad happened with writing to file!\n";
        return;
    }

    for (const auto& word_entry: words) {
        string word = word_entry.first;
        vector<int> file_ids = word_entry.second;

        fout << word << ":[";
        if (!file_ids.empty()) {
            for (int i = 0; i < file_ids.size() - 1; i++)
                fout << file_ids[i] << " ";
            fout << file_ids[file_ids.size() - 1] << "]\n";
        }
    }
    fout.close();
}

int compare(const pair<string, vector<int>>& word1, const pair<string, vector<int>>& word2) {
    if (word1.second.size() == word2.second.size()) 
        return word1.first < word2.first;
    return word1.second.size() > word2.second.size();
}

void write_files(reduce_data* reduce_data) {
    while (true) {
        pthread_mutex_lock(&reduce_data->mutex);
        if (reduce_data->letters->empty()) {
            pthread_mutex_unlock(&reduce_data->mutex);
            break;
        }

        string letter = reduce_data->letters->back();
        reduce_data->letters->pop_back();
        pthread_mutex_unlock(&reduce_data->mutex);
        
        vector<pair<string, vector<int>>> words;
        unordered_map<string, vector<int>> words_by_letter = reduce_data->final_lists[letter];
        for (const auto& pair : words_by_letter) {
            string word = string(pair.first);
            vector<int> file_ids = vector<int>(pair.second);
            sort(file_ids.begin(), file_ids.end());
            words.push_back({word, file_ids});
        }
        
        sort(words.begin(), words.end(), compare);
        write_words_to_file(letter, words);
    }
}

void aggregate_words(map_data* map_data, reduce_data* reduce_data, int thread_id) {
    int no_of_elements = map_data->partial_lists.size();
    int elements_for_single_thread = no_of_elements / reduce_data->no_of_reducers;
    int start = thread_id * elements_for_single_thread;
    int end = start + elements_for_single_thread - 1;
    if (thread_id == reduce_data->no_of_reducers - 1) {
        end = no_of_elements - 1;
    }

    unordered_map<string, vector<int>> aggregated_list;
    for (int i = start; i <= end; i++) {
        pair<string, int> entry = map_data->partial_lists[i];
        string word = entry.first;
        int file_id = entry.second;

        if (aggregated_list.find(word) == aggregated_list.end())
            aggregated_list[word] = vector<int>();
        aggregated_list[word].push_back(file_id);
    }

    pthread_mutex_lock(&reduce_data->mutex);
    for (const auto& pair: aggregated_list) {
        string word = pair.first;
        string letter = string(1, word[0]);

        if (reduce_data->final_lists.find(letter) == reduce_data->final_lists.end())
            reduce_data->final_lists[letter] = unordered_map<string, vector<int>>();
        
        if (reduce_data->final_lists[letter].find(word) == reduce_data->final_lists[letter].end()) {
            reduce_data->final_lists[letter][word] = vector<int>(pair.second);
        } else {
            for (int element: pair.second)
                reduce_data->final_lists[letter][word].push_back(element);
        } 
    }
    pthread_mutex_unlock(&reduce_data->mutex);
}

void* reduce_function(void *arg) {
    reduce_thread* thread_data = (reduce_thread*)arg;
    map_data* map_data = thread_data->map_data_for_reducing;
    reduce_data* reduce_data = thread_data->reducing_data;

    pthread_barrier_wait(&map_data->barrier); // wait for MAP threads to finish
    aggregate_words(map_data, reduce_data, thread_data->thread_id);
    pthread_barrier_wait(&reduce_data->barrier);
    write_files(reduce_data);

    return NULL;
}

void process_file(string file, int file_id, map_data* map_data) {
    ifstream fin(file);
    if (!fin.is_open()) {
        cout << "Error: something bad happened with processing the file!\n";
        return;
    }

    stringstream buffer;
    buffer << fin.rdbuf();
    string content = buffer.str();
    fin.close();

    unordered_map<string, bool> unique_words;
    stringstream ss(content);
    string word;

    while (ss >> word) {
        string processed_word = "";
        for (char letter: word) {
            if (isalpha(letter)) {
                if (isupper(letter)) {
                    processed_word += tolower(letter);
                } else {
                    processed_word += letter;
                }
            }
        }
    
        if (processed_word != "")
            unique_words[processed_word] = true;
    }

    pthread_mutex_lock(&map_data->mutex);
    for (auto word: unique_words) 
        map_data->partial_lists.push_back({word.first, file_id});
    pthread_mutex_unlock(&map_data->mutex);
}

void* map_function(void *arg) {
    map_thread* thread_data = (map_thread*)arg;
    map_data* map_data = thread_data->data;

    while (true) {
        pthread_mutex_lock(&map_data->mutex);
        if (map_data->files->empty()) {
            pthread_mutex_unlock(&map_data->mutex);
            break;
        }

        int file_id = map_data->files->size();
        string file = map_data->files->back();
        map_data->files->pop_back();
        pthread_mutex_unlock(&map_data->mutex);

        process_file(file, file_id, map_data);
    }
    
    pthread_barrier_wait(&map_data->barrier);

    return NULL;
}  

vector<string> get_files(string input_file) {
    vector<string> files_to_process;

    ifstream fin(input_file);
    if (!fin.is_open()) {
        cout << "Error: something bad happened with reading the file!\n";
        return files_to_process;
    }

    int no_files_to_process;
    string file;

    fin >> no_files_to_process;
    for (int i = 0; i < no_files_to_process; i++) {
        fin >> file;
        files_to_process.push_back(file);
    }

    fin.close();
    return files_to_process;
}

int main(int argc, char *argv[]) {
    int M = atoi(argv[1]);
    int R = atoi(argv[2]);
    string input_file = argv[3];

    int no_of_threads = M + R;
    pthread_t threads[no_of_threads];

    vector<string> files_to_process = get_files(input_file);

    map_data map_data_args;
    map_data_args.files = &files_to_process;
    map_data_args.partial_lists = vector<pair<string, int>>();
    pthread_mutex_init(&map_data_args.mutex, NULL);
    pthread_barrier_init(&map_data_args.barrier, NULL, M + R);

    map_thread map_threads[M];

    for (int i = 0; i < M; i++) {
        map_threads[i].data = &map_data_args;
        map_threads[i].thread_id = i;

        int ret = pthread_create(&threads[i], NULL, map_function, &map_threads[i]);
        if (ret != 0) {
            cout << "Error: something bad happened with map_thread!\n";
            return -1;
        }
    }

    vector<string> letters;
    for (char letter = 'a'; letter <= 'z'; letter++)
        letters.push_back(string(1, letter));

    reduce_data reduce_data_args;
    reduce_data_args.no_of_reducers = R;
    reduce_data_args.mapping_data = &map_data_args;
    reduce_data_args.letters = &letters;
    reduce_data_args.final_lists = unordered_map<string, unordered_map<string, vector<int>>>();;
    pthread_mutex_init(&reduce_data_args.mutex, NULL);
    pthread_barrier_init(&reduce_data_args.barrier, NULL, R);

    reduce_thread reduce_threads[R];

    for (int i = 0; i < R; i++) {
        reduce_threads[i].map_data_for_reducing = &map_data_args;
        reduce_threads[i].reducing_data = &reduce_data_args;
        reduce_threads[i].thread_id = i;

        int ret = pthread_create(&threads[M + i], NULL, reduce_function, &reduce_threads[i]);
        if (ret != 0) {
            cout << "Error: something bad happened with reduce_thread!\n";
            return -1;
        }
    }

    for (int i = 0; i < no_of_threads; i++) {
        int ret = pthread_join(threads[i], NULL);
        if (ret != 0) {
            cout << "Error: something bad happened with joining the thread\n";
            return -1;
        }
    }

    pthread_mutex_destroy(&map_data_args.mutex);
    pthread_mutex_destroy(&reduce_data_args.mutex);
    pthread_barrier_destroy(&map_data_args.barrier);
    pthread_barrier_destroy(&reduce_data_args.barrier);

    return 0;
}