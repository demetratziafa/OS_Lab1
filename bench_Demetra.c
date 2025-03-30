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

struct kiwi_str{

	long int count;
	int r;
	int per;
	long long start, end;
};


void * my_write_test(void *arg);
void * my_read_test(void *arg);
void print_statistics(char * mode, void* arg1);

int main(int argc,char** argv)
{

	struct kiwi_str *wr, *re;
	srand(time(NULL));
	if (argc < 3) {
		fprintf(stderr,"Usage: db-bench <write | read> <count>\n");
		exit(1);
	}
	
	if (strcmp(argv[1], "write") == 0) {
		wr = (struct kiwi_str*)malloc(sizeof(struct kiwi_str));
		wr->count = atoi(argv[2]);
		_print_header(wr->count);
		_print_environment();
		if (argc == 4)
			wr->r = 1;
		if(argc == 5){
			wr->per = atoi(argv[4]);

		}else{
			wr->per = 0;
		}	
		my_write_test(wr);
		print_statistics(argv[1],wr);
	} else if (strcmp(argv[1], "read") == 0) {
		re = (struct kiwi_str*)malloc(sizeof(struct kiwi_str));
		re->count = atoi(argv[2]);
		_print_header(re->count);
		_print_environment();
		if (argc == 4)
			re->r = 1;
		if(argc == 5){
			re->per = atoi(argv[4]);	
		}else{
			re->per = 0;
		}	
		my_read_test(re);
		print_statistics(argv[1],re);
	}/*else if(strcmp(argv[1], "readwrite") == 0){
		wr = (struct kiwi_str*)malloc(sizeof(struct kiwi_str));
		re = (struct kiwi_str*)malloc(sizeof(struct kiwi_str));
		if(argc<5){
			fprintf(stderr,"Usage: db-bench <readwrite> <count> <r> <percentageofwrites> <percentageofreads>\n");
			exit(1);
		}
		wr->count = atoi(argv[2]);
		re->count = atoi(argv[2]);
		_print_header(wr->count);
		_print_environment();
		if (argc == 6){
			wr->r = 1;
			re->r = 1;
			wr->per = atoi(argv[4]);
			re->per = atoi(argv[5]);
			
		}else if(argc == 5){
			wr->r = 0;
			wr->r = 0;
			wr->per = atoi(argv[3]);
			re->per = atoi(argv[4]);
			}		
		my_write_test(wr);
		my_read_test(re); */
		
	 else {
		fprintf(stderr,"Usage: db-bench <write | read> <count> <random>\n");
		exit(1);
	}

	return 1;
}
