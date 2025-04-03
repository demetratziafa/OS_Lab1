#include "bench.h"

void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

void _print_header(int count)
{
	double index_size = (double)((double)(KSIZE + 8 + 1) * count) / 1048576.0;
	double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

	printf("Keys:\t\t%d bytes each\n", 
			KSIZE);
	printf("Values: \t%d bytes each\n", 
			VSIZE);
	printf("Entries:\t%d\n", 
			count);
	printf("IndexSize:\t%.1f MB (estimated)\n",
			index_size);
	printf("DataSize:\t%.1f MB (estimated)\n",
			data_size);

	printf(LINE1);
}

void _print_environment()
{
	time_t now = time(NULL);

	printf("Date:\t\t%s", 
			(char*)ctime(&now));

	int num_cpus = 0;
	char cpu_type[256] = {0};
	char cache_size[256] = {0};

	FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
	if (cpuinfo) {
		char line[1024] = {0};
		while (fgets(line, sizeof(line), cpuinfo) != NULL) {
			const char* sep = strchr(line, ':');
			if (sep == NULL || strlen(sep) < 10)
				continue;

			char key[1024] = {0};
			char val[1024] = {0};
			strncpy(key, line, sep-1-line);
			strncpy(val, sep+1, strlen(sep)-1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strcpy(cpu_type, val);
			}
			else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);	
		}

		fclose(cpuinfo);
		printf("CPU:\t\t%d * %s", 
				num_cpus, 
				cpu_type);

		printf("CPUCache:\t%s\n", 
				cache_size);
	}
}
//me ta structs 
			 
struct kiwi_write{

	long int count;
	int r;
	DB* db;

};


struct kiwi_read{

	long int count;
	int r;
	DB* db;
	
};


void * my_write_test(void *arg); //me
void * my_read_test(void *arg); //me
void print_statistics(char * mode, double cost, void* arg1); //me

int main(int argc,char** argv)
{
	long long start,end; //me
	double cost; //me
	struct kiwi_write *wr = (struct kiwi_write*)malloc(sizeof(struct kiwi_write)); //me
	srand(time(NULL));
	if (argc < 3) {
		fprintf(stderr,"Usage: db-bench <write | read> <count>\n");
		exit(1);
	}
	
	if (strcmp(argv[1], "write") == 0) {
		start = get_ustime_sec();  //me
		wr->count = atoi(argv[2]); //me
		wr->db = db_open(DATAS); //me
		_print_header(wr->count);
		_print_environment();
		if (argc == 4){
			wr->r = 1;
		}		
		my_write_test(wr); //me
		db_close(wr->db); //me
		end = get_ustime_sec(); //me
		cost = end - start;//me
		print_statistics("write", cost, wr); //me
	} else if (strcmp(argv[1], "read") == 0) {
		struct kiwi_read *re = (struct kiwi_read*)malloc(sizeof(struct kiwi_read)); //me
		start = get_ustime_sec(); //me
		re->count = atoi(argv[2]); //me
		re->db = db_open(DATAS); //me
		_print_header(re->count);
		_print_environment();
		if (argc == 4)
			re->r = 1;	
		my_read_test(re); 
		db_close(re->db); //me
		end = get_ustime_sec(); //me
		cost = end - start; //me
		print_statistics("read", cost, re); //me
		
		
	} else if(strcmp(argv[1], "readwrite") == 0){ //apo edo kai kato //me
	

		if(argc<4){
			fprintf(stderr,"Usage: db-bench <readwrite> <count> <percentageofwrites> <percentageofreads>\n");
			exit(1);
		}
		struct kiwi_read *re = (struct kiwi_read*)malloc(sizeof(struct kiwi_read));
		wr->r = 0;
		re->r = 0;
		int writeper = atoi(argv[3]);
		int readper = atoi(argv[4]);
		
		
		DB* db = db_open(DATAS);
    		wr->db = db;
    		re->db = db;
		long long count = atol(argv[2]);
		wr->count = (writeper*count)/100;
		re-> count = (readper*count)/100; 
		_print_header(count);
		_print_environment();
		
		pthread_t write;
		pthread_t read;


		pthread_create(&write, NULL, my_write_test, (void*) wr);
		pthread_create(&read, NULL, my_read_test, (void*) re);

		pthread_join(write, NULL);
		pthread_join(read, NULL);
		
		db_close(re->db);
		free(wr);
		free(re);
		
	 }else {
		fprintf(stderr,"Usage: db-bench <write | read> <count> <random>\n");
		exit(1);
          }

	return 0;
}
