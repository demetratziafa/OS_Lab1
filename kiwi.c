#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"

//#define DATAS ("testdb")

void _write_test(long int count, int r)
{
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
	for (i = 0; i < count; i++) {
		if (r)
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

	end = get_ustime_sec();
	cost = end -start;

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, cost /(double) count
		,count /(double) cost
		,cost);	
}

void _read_test(long int count, int r)
{
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
	for (i = 0; i < count; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (r)
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

	end = get_ustime_sec();
	cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		cost /(double) count,
		count /(double) cost,
		cost);
}


//structs ton pedion ton arxikon functions read kai write +deikti sto Db gia open/close database
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

//sinartisi ektiposis statistikon apodosis kathe leitoyrgias	
void print_statistics(char * mode, double cost, void* arg1,int wp, int rp){
	 
	
	if(strcmp(mode,"write")==0){
		struct kiwi_write *wr = (struct kiwi_write*)arg1;
		printf(LINE);
		printf("Write Statistics\n");
		printf("Total Number of Writes:%ld\n", wr->count);
		printf("%.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
			,cost /(double) wr->count
			,wr->count /(double) cost
			,cost); 	
				
	}else if(strcmp(mode,"read")==0){
		struct kiwi_read *re = (struct kiwi_read*)arg1;
		printf(LINE);
		printf("Read Statistics\n");	
		printf("Total Number of Reads:%ld\n", re->count);
		printf("%.6f sec/op; %.1f reads/sec(estimated); cost:%.3f(sec);\n"
			,cost /(double) re->count
			,re->count /(double) cost
			,cost); 
	}else if(strcmp(mode,"readwrite")==0){
		struct kiwi_write *rw = (struct kiwi_write*)arg1;
		double rper = (rp*(rw->count))/100;
		double wper = (wp*(rw->count))/100;
		printf("Read Statistics\n");
		printf("Total Number of Reads:%f\n", rper);
		printf("%d%% of the total number of reads read:%f\n",rp, rper);
		printf("%.6f sec/op; %.1f reads/sec(estimated); cost:%.3f(sec);\n"
		,cost /(double) rper
		,rper /(double) cost
		,cost); 
		printf("Write Statistics\n");
		printf("Total Number of Writes:%ld\n", rw->count);
		printf("%d%% of the total number of reads read:%f\n",wp, wper);
		printf("%.6f sec/op; %.1f reads/sec(estimated); cost:%.3f(sec);\n"
		,cost /(double) wper
		,wper /(double) cost
		,cost); 	
		}				
} 
					

void * my_write_test(void *arg)
{
	struct kiwi_write *wr = (struct kiwi_write*)arg; //deiktis toy write struct
	int i;
	Variant sk, sv;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);
	

	for (i = 0; i < wr->count; i++) {
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

		db_add(wr->db, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	return NULL; 
}

void * my_read_test(void *arg)
{
	struct kiwi_read *re = (struct kiwi_read*)arg; //deiktis toy read struct
	int i;
	int ret;
	int found = 0;
	Variant sk;
	Variant sv;
	char key[KSIZE + 1];

	for (i = 0; i < re->count; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (re->r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;
		ret = db_get(re->db, &sk, &sv);
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

 	return NULL;	
}

