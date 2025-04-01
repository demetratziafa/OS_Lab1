#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h" //we also need this for the struct

#define DATAS ("testdb")

//( our ) changing write test
//afinw read, write opws htan arxika kai kanw tis allages stis myread, my write
void _write_test(void *arg)
{
	//our struct
	struct data *d = (struct data *) arg;	
	
	int i;
	double cost;
	long long start,end;
	Variant sk, sv;
	DB* db;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	db = db_open(DATAS);

	start = get_ustime_sec();

	for (i = 0; i < d->count; i++) {
		if (d->r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d adding %s\n", i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

		db_add(d->db, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	end = get_ustime_sec();
	cost = end -start;

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, cost /(double) count
		,count /(double) cost
		,cost);	
}

//( our ) changing read test
void _read_test(void *arg)
{
	//our struct
	struct data *d = (struct data *) arg;

	int i;
	int ret;
	int found = 0;
	double cost;
	long long start,end;
	Variant sk;
	Variant sv;
	DB* db;
	char key[KSIZE + 1];

	db = db_open(DATAS);
	start = get_ustime_sec();


	for (i = 0; i < d->count; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (d->r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;
		ret = db_get(d->db, &sk, &sv);
		if (ret) {
			//db_free_data(sv.mem);
			found++;
		} else {
			INFO("not found key#%s", 
					sk.mem);
    	}

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	end = get_ustime_sec();
	cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		cost /(double) count,
		count /(double) cost,
		cost);
}

//our
struct kiwi_str{

	long int count;
	int r;
	int per;
	long long start, end;
};

	
void print_statistics(char * mode, void* arg1){
	
	struct kiwi_str *rw = (struct kiwi_str*)arg1;
	long int countwr;
	double cost = rw->end - rw->start;
	
	if(strcmp(mode,"write")==0){
		printf(LINE);
		printf("Write Statistics\n");
		if(rw->per == 0){
			printf("Total Number of Writes:%ld\n", rw->count);
			printf("%.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
			,cost /(double) rw->count
			,rw->count /(double) cost
			,cost); 
		}else{
			countwr = (rw->per)*(rw->count)/100;
			printf("Total Number of Writes:%ld\n", rw->count);
			printf("%d%% of the total number of writes added:%ld\n", rw->per, countwr);
			printf("%.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
			,cost /(double) countwr
			,countwr /(double) cost
			,cost); 	
		}				
	}else if(strcmp(mode,"read")==0){
		printf(LINE);
		printf("Read Statistics\n");
		if(rw->per == 0){
			printf("Total Number of Reads:%ld\n", rw->count);
			printf("%.6f sec/op; %.1f reads/sec(estimated); cost:%.3f(sec);\n"
			,cost /(double) rw->count
			,rw->count /(double) cost
			,cost); 
		}else{
			countwr = (rw->per)*(rw->count)/100;
			printf("Total Number of Reads:%ld\n", rw->count);
			printf("%d%% of the total number of reads read:%ld\n", rw->per, countwr);
			printf("%.6f sec/op; %.1f reads/sec(estimated); cost:%.3f(sec);\n"
			,cost /(double) countwr
			,countwr /(double) cost
			,cost); 	
		}				
	}
}				
		

void * my_write_test(void *arg)
{
	struct kiwi_str *wr = (struct kiwi_str*)arg;
	int i;
	Variant sk, sv;
	DB* db;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];
	long int countw;

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	db = db_open(DATAS);
	if(wr->per == 0){
		countw = wr->count;
	}else{
		countw = (wr->per)*(wr->count)/100;
	}		
		

	wr->start = get_ustime_sec();
	for (i = 0; i < countw; i++) {
		if (wr->r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d adding %s\n", i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

		db_add(db, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	wr->end = get_ustime_sec();
	return NULL;	
}

void * my_read_test(void *arg)
{
	struct kiwi_str *re = (struct kiwi_str*)arg;
	int i;
	int ret;
	int found = 0;
	Variant sk;
	Variant sv;
	DB* db;
	char key[KSIZE + 1];
	long int countr;

	db = db_open(DATAS);
	if(re->per == 0){
		countr = re->count;
	}else{
		countr = (re->per)*(re->count)/100;
	}
	re->start = get_ustime_sec();
	for (i = 0; i < countr; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (re->r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;
		ret = db_get(db, &sk, &sv);
		if (ret) {
			//db_free_data(sv.mem);
			found++;
		} else {
			INFO("not found key#%s", 
					sk.mem);
    	}

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	re->end = get_ustime_sec();
	return NULL;	
}
