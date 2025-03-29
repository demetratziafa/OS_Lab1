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
void print_statistics(double costw, double costr, int writeper, int readper, long int count){
	int readnum = (readper*count)/100;
	int writenum = (writeper*count)/100;
	printf(LINE);
	printf("Number Of Requests:%ld\n", count);
	printf("Number of Writes(%d%% of total number of requests):%d\n", writeper, writenum); 
	printf("|Random-Write	(done:%d): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,writenum,
		writenum /(double) costw, 
		writenum /(double) costw, 
		costw);
	printf("Number of Reads(%d%% of total number of requests):%d\n", readper, writeper); 	
	printf("|Random-Read	(done:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		readnum,
		readnum /(double) costr, 
		readnum /(double) costr,  
		costr); 
}		


void* my_write_test(void* arg);
void* my_read_test(void* arg);


struct kiwi_str{
	long int count;
	int r;
};	
	
int main(int argc,char** argv)
{
	//long int count;
	//int writeper, readper;
	//double costw, costr;
	struct kiwi_str *dw, *dr;
	pthread_t write1;
	pthread_t read1;
	

	srand(time(NULL));
	if (argc < 3) {
		fprintf(stderr,"Usage: db-bench <write | read | readwrite>  <count>\n");
		exit(1);
	}
	
	if (strcmp(argv[1], "write") == 0) {
		//int r = 0;
		dw = (struct kiwi_str*) malloc(sizeof(struct kiwi_str));
		dw->count = atoi(argv[2]);
		_print_header(dw->count);
		_print_environment();
		if (argc == 4)
			dw->r = 1;
		pthread_create(&write1, NULL, my_write_test, (void*) dw);
    		pthread_join(write1, NULL);	
	} else if (strcmp(argv[1], "read") == 0) {
		//int r = 0;
		dr = (struct kiwi_str*) malloc(sizeof(struct kiwi_str));
		dr->count = atoi(argv[2]);
		_print_header(dr->count);
		_print_environment();
		if (argc == 4)
			dr->r = 1;
		pthread_create(&read1, NULL, my_read_test, (void*) dr);
		pthread_join(read1,NULL);
	}/*else if(strcmp(argv[1], "readwrite") == 0){
		int r = 0;
		if(argc<5){
			fprintf(stderr,"Usage: db-bench <readwrite> <count> <percentageofwrites> <ofreads> <r>\n");
			exit(1);
		}
		count = atoi(argv[2]);
		writeper = atoi(argv[3]);
		readper = atoi(argv[4]);
		_print_header(count); 
		_print_environment();
		if (argc == 6){
			r = 1;
		}
		costw = my_write_test(count, r, writeper);
		_print_header(count); 
		_print_environment();
		costr = my_read_test(count, r, readper);
		print_statistics(costw, costr, writeper, readper, count); 
		_readwrite_test(count, r, writeper, readper);	*/	
	 else {
		fprintf(stderr,"Usage: db-bench <write | read | readwrite> <count> <random>\n");
		exit(1);
	}

	return 1;
}
