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

//structs gia tis read,write orates gia ti main 
			 
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

//function prototypes
void * my_write_test(void *arg); 
void * my_read_test(void *arg); 
void print_statistics(char * mode, double cost, void* arg1,int wp, int rp);

int main(int argc,char** argv)
{
	long long start,end; //prosthiki edo gia katametrisi ton statistikon logo toy open/close database
	double cost; //prosthiki edo gia ta statistika
	struct kiwi_write *wr = (struct kiwi_write*)malloc(sizeof(struct kiwi_write)); //global deiktis sto write struct logo toy enos grafea
	srand(time(NULL));
	if (argc < 3) {
		fprintf(stderr,"Usage: db-bench <write | read> <count>\n");
		exit(1);
	}
	
	if (strcmp(argv[1], "write") == 0) {
		start = get_ustime_sec();  
		wr->count = atoi(argv[2]); 
		wr->db = db_open(DATAS); //anoigma vasis
		_print_header(wr->count);
		_print_environment();
		if (argc == 4){
			wr->r = 1;
		}		
		my_write_test(wr); //kleisi dikis mas write
		db_close(wr->db); //anoigma vasis
		end = get_ustime_sec(); 
		cost = end - start;
		print_statistics("write", cost, wr,0,0); //ektiposi statistikon apodosis
		free(wr); //apodesmeusi mnimis
	} else if (strcmp(argv[1], "read") == 0) {
		struct kiwi_read *re = (struct kiwi_read*)malloc(sizeof(struct kiwi_read)); //deiktis sto read struct
		start = get_ustime_sec(); 
		re->count = atoi(argv[2]); 
		re->db = db_open(DATAS); 
		_print_header(re->count);
		_print_environment();
		if (argc == 4)
			re->r = 1;	
		my_read_test(re); //kleisi dikis mas read
		db_close(re->db); //kleisimo vasis
		end = get_ustime_sec(); 
		cost = end - start; 
		print_statistics("read", cost, re,0,0); //ektiposi statistikon apodosis
		free(re); //apodesmeusi mnimis
		
		
	} else if(strcmp(argv[1], "readwrite") == 0) //ilopoiisi mnimis
	{ 
	
		if(argc<4){
			fprintf(stderr,"Usage: db-bench <readwrite> <count> <percentageofwrites> <percentageofreads>\n");
			exit(1); //minima se periptosi poy den dosei sosta args o user 
		}
		start = get_ustime_sec();
		struct kiwi_read *re = (struct kiwi_read*)malloc(sizeof(struct kiwi_read)); //local deiktis sto read struct logo ton pollon grafeon
		wr->r = 0;
		re->r = 0;
		int writeper = atoi(argv[3]); //pososto gia write
		int readper = atoi(argv[4]);  //pososto gia read
		
		
		DB* db = db_open(DATAS); //anoigma vasis
    		wr->db = db;
    		re->db = db;
		long long count = atol(argv[2]);
		wr->count = (writeper*count)/100; //airthmos eggrafon poy tha ektelesei i write
		re-> count = (readper*count)/100;  //arithmos eggrafon poy tha ektelesei i read
		_print_header(count);
		_print_environment();
		
		pthread_t write;
		pthread_t read[2];


		pthread_create(&write, NULL, my_write_test, (void*) wr); //dimiourgia nimatos gia write
		for(int i=0;i<2;i++){
			pthread_create(&read[i], NULL, my_read_test, (void*) re); //dimioyrgia nimaton gia read
		}
		pthread_join(write, NULL);
		for(int i=0;i<2;i++){
			pthread_join(read[i], NULL);
		}
		
		db_close(re->db); //kleisimo vasis
		end = get_ustime_sec(); 
		cost = end - start; 
		//print_statistics("readwrite", cost, re,writeper,readper);
		free(wr);
		free(re);
		
	 }else {
		fprintf(stderr,"Usage: db-bench <write | read> | <readwrite> <count> <random>\n");
		exit(1);
          }

	return 0;
}
