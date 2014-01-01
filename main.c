//
//  main.c
//  pThread_PageRank
//
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "math.h"
#include <time.h>
#include <pthread.h>
#include <sys/time.h>


void *print_message_function( void *ptr );
void *populate_P(void *ptr);
void *organise_task(void *ptr);
void *crawl_task(void *ptr);
void *quicksort(int *sort_index_array,int* mark_array,int left,int right);
int partition(int* sort_index_array,int* DataBlock, int left, int right, int pivotIndex);
void *quicksort_threaded(void *ptr);


struct barrier{
    int count;
    pthread_mutex_t lock_count;
    pthread_cond_t GoUp, GoDown;
};

struct barrier *my_log_barrier;

void barrier_init(struct barrier *my_log_barrier, int num_of_threads){
    int i;
    for(i=0;i<num_of_threads;i++)
    {
        my_log_barrier[i].count=0;
        pthread_mutex_init(&my_log_barrier[i].lock_count, NULL);
        pthread_cond_init(&my_log_barrier[i].GoUp, NULL);
        pthread_cond_init(&my_log_barrier[i].GoDown, NULL);
    }
}

void log_barrier_function(struct barrier *my_log_barrier, int thread_id,int num_of_threads){
    int i = 2;
    int j = 0;
    int k;
    do{
        k = j+i/2;
        
        if(thread_id%i == 0)
        {
            printf("thread %d lock \n", thread_id);
            pthread_mutex_lock(&my_log_barrier[k].lock_count);
            my_log_barrier[k].count++;
            while(my_log_barrier[k].count<2)
                pthread_cond_wait(&my_log_barrier[k].GoUp, &my_log_barrier[k].lock_count);
            printf("thread %d went up \n", thread_id);
            
            pthread_mutex_unlock(&my_log_barrier[k].lock_count);
            
        }
        
        else{
            printf("thread %d lock \n", thread_id);
            pthread_mutex_lock(&my_log_barrier[k].lock_count);
            my_log_barrier[k].count++;
            if(my_log_barrier[k].count == 2)
            {
                pthread_cond_signal(&my_log_barrier[k].GoUp);
            printf("thread %d issued goup \n", thread_id);
            }
            while(my_log_barrier[k].count!=0)
            {
                printf("waiting \t");

                pthread_cond_wait(&my_log_barrier[k].GoDown, &my_log_barrier[k].lock_count);
            
            }
            printf("thread %d iwent down \n", thread_id);
            
            pthread_mutex_unlock(&my_log_barrier[k].lock_count);
            break;
            
        }
        
        j += num_of_threads/i;
        
        i *= 2;
        
    }while(i<=num_of_threads);
    
    for(i=i/2;i>1;i=i/2)
    {
        j = j - num_of_threads/i;
        k = j + thread_id/i;
        
        pthread_mutex_lock(&my_log_barrier[k].lock_count);
        my_log_barrier[k].count = 0;
        pthread_mutex_unlock(&my_log_barrier[k].lock_count);

        pthread_cond_broadcast(&my_log_barrier[k].GoDown);

        printf("thread %d issued go down \n", thread_id);
        
        
    }
}

struct ThreadArgument{
    float** DataBlock;
    struct node_element *node_info;
};

struct CrawlThreadArgument{
    int *mark;
    int *crawler;
    struct node_element *node_info;
    float** DataBlock;
    int NumOfNodes;
    int tid;
    int num_of_threads;
};

struct SortThreadArgument{
    int *sort_index_array;
    int *mark_array;
    int left;
    int right;
    int tid;
};

struct node_element {
    int num_of_nodes;
    int index;
};

void heapify(int* a,int *DataArray,int count);
void heapSort(int* a,int *DataArray, int count);
void swap(int *left,int *right);
void siftDown(int *a, int *DataArray,int start,int end);


int N;
int NumOfLines = 0;
int NumberOfSteps = 0;

int main(int argc, const char * argv[])
{
    NumberOfSteps = atoi(argv[2]);
    int a1[10];
    int MyDataArray[10];
    
    
    float** P1;
    int i,j,k;
    int i_sparse = 0;
    clock_t start, end;
    int *mark;
    int *sorted_index;
    int *crawler;
    int repeat = 0;
    int index = 0;
    float temp,temp1,temp2;
    int highest_hundred[100]={0};
    int nodes_per_thread = 0;
    int rem;
    
    /*Thread Related stuff*/
    int threads = atoi(argv[3]);
    pthread_t thread_local[threads];
    pthread_t thread_local1[threads];
    my_log_barrier = (struct barrier*)malloc(threads*sizeof(my_log_barrier));
    
    
    
    struct CrawlThreadArgument CrawlArgumentStruct[threads];
    struct SortThreadArgument SortArgumentStruct[threads];
    
    
    pthread_t thread1, thread2;
    char *message1 = "Thread 1";
    char *message2 = "Thread 2";
    int  iret1, iret2;
    
    struct node_element *node_info;
    
    struct timeval start_time, finish_time,start_time1,finish_time1;
    double elapsed;
    
    /*Thread Related stuff ends*/
    
    
    
    /* File related stuff **/
    
    
    const char * filename = argv[1];
   // const char *outfile = argv[2];
    
    FILE * pFile;
    FILE * pFile1;
    pFile = fopen (filename,"r");
    pFile1 = pFile;
    fpos_t pos;
    char ch;
    
    /* File related stuff **/
    
    /*
     for(i=0;i<10;i++)
     {
     MyDataArray[i] = rand()%100;
     a1[i] = i;
     }
     
     for(i=0;i<10;i++)
     {
     printf("a1 = %d \t, MyData = %d \n",a1[i],MyDataArray[a1[i]]);
     }
     
     heapSort(a1,MyDataArray,10);
     
     for(i=0;i<10;i++)
     {
     printf("a1 = %d \t, MyData = %d \n",a1[i],MyDataArray[a1[i]]);
     }
     
     exit(0);
     */
    
    if(pFile==NULL)
    {
        printf("File Read Error \n");
        return 0;
    }
    
    
    fgetpos (pFile,&pos);
    
 //   printf("File Opened \n");
    
    while(!feof(pFile)){
        ch = fgetc(pFile);
        if( ch== '\n')
            NumOfLines++;        
    }
    
    
    NumOfLines++;
    
//    printf("Number of Lines = %d\n",NumOfLines);
    fsetpos(pFile, &pos);
    
    
    P1 = (float**)malloc( NumOfLines*sizeof( float* ));
    
    
    for(i=0;i<NumOfLines;i++)
    {
        P1[i] = ( float* )malloc( 3* sizeof( float));
    }
    
    
    
    while(!feof(pFile)){
        fscanf (pFile, "%f",  &temp);
        fscanf (pFile, "%f", &temp1);
        fscanf (pFile, "%f", &temp2);
        
        P1[i_sparse][0] = temp;
        P1[i_sparse][1] = temp1;
        P1[i_sparse][2] = 0.85*temp2;//+OneByNTimesE;        
        i_sparse++;
    }
    
    
 //   printf("i_sparse = %d \n", i_sparse);
    i_sparse = 0;
    
    N = (int)P1[NumOfLines-1][1];
    N++;
    
//    printf("Number of Nodes = %d \n",N);
    
    mark = (int*) malloc(N*sizeof(int));
 //   printf("Mark init DOne \n");
    
    crawler = (int*)malloc(N*sizeof(int));
    sorted_index = (int*)malloc(N*sizeof(int));
    
    for(i=0;i<N;i++)
    {
        mark[i] = 0;
        crawler[i] = i;
        sorted_index[i] = i;
    }
    
//    printf("Crawl init DOne \n");
    
    node_info = malloc(N*sizeof(node_info));
//    printf("nodeinfo init DOne \n");
    
    
    for(i_sparse=0;i_sparse<NumOfLines-1;i_sparse++)
    {
        
        if(P1[i_sparse][1] == P1[i_sparse+1][1])
        {
            repeat++;
        }
        
        else
        {
            node_info[(int)P1[index][1]].num_of_nodes = ++repeat;
            node_info[(int)P1[index][1]].index = index;
            repeat = 0;
            index = i_sparse+1;
        }
        
    }
    
//    printf("Loop DOne \n");
    
    node_info[(int)P1[i_sparse][1]].num_of_nodes = ++repeat;
    node_info[(int)P1[i_sparse][1]].index = index;
    
    
    
 //   printf("Start Crawling\n");
    
    start = clock();
    gettimeofday(&start_time, NULL);
    
    
#if (1)
    for(i=0;i<NumberOfSteps;i++)
    {
        for(k=0;k<N;k++)
        {
            mark[crawler[k]]++;
            int r = (int)(drand48()*(node_info[crawler[k]].num_of_nodes));
            crawler[k] = (int)P1[node_info[crawler[k]].index+r][0];
            
        }
    }
    quicksort(sorted_index,mark, 0, N-1);
    
    end = clock();
    gettimeofday(&finish_time, NULL);
    
 //   printf("Serial output: \n Top 100 Nodes in descending order \n ");
    
 //   for(i=N-1;i>N-101;i--)
 //       printf("index = %d \t, mark = %d \n", sorted_index[i],mark[sorted_index[i]]);
 //   double elapsed_time = (end-start)/(double)CLOCKS_PER_SEC ;
    
    double elapsed1 = ((double)(finish_time.tv_usec - start_time.tv_usec)/1000000.0)+((double)(finish_time.tv_sec - start_time.tv_sec));
    
 //   printf("Serial Time = %lf \n\n",elapsed_time);
    printf("Time for serial execution = %lf \n",elapsed1);
    
    
#endif
    
    
#if 1
    
    barrier_init(my_log_barrier,threads);

    
    for(i=0;i<N;i++)
    {
        mark[i] = 0;
        crawler[i] = i;
        sorted_index[i] = i;
    }
    
    
    rem = N%threads;
    
    nodes_per_thread = N/threads;
    gettimeofday(&start_time1, NULL);
    
    
 //   for(j=0;j<NumberOfSteps;j++)
   // {
        for(i=0;i<threads-1;i++)
        {
            
            CrawlArgumentStruct[i].crawler = &crawler[i*nodes_per_thread];
            CrawlArgumentStruct[i].DataBlock = P1;
            CrawlArgumentStruct[i].mark = mark;
            CrawlArgumentStruct[i].node_info = node_info;
            CrawlArgumentStruct[i].NumOfNodes = nodes_per_thread;
            CrawlArgumentStruct[i].tid = i;
            CrawlArgumentStruct[i].num_of_threads = threads;
            pthread_create( &thread_local[i], NULL, crawl_task, (void*) &CrawlArgumentStruct[i]);
            
        }
        
        
        
        CrawlArgumentStruct[i].crawler = &crawler[i*nodes_per_thread];
        CrawlArgumentStruct[i].DataBlock = P1;
        CrawlArgumentStruct[i].mark = mark;
        CrawlArgumentStruct[i].node_info = node_info;
        CrawlArgumentStruct[i].NumOfNodes = nodes_per_thread+rem;
        CrawlArgumentStruct[i].num_of_threads = threads;
        CrawlArgumentStruct[i].tid = i;
        
        pthread_create( &thread_local[i], NULL, crawl_task, (void*) &CrawlArgumentStruct[i]);
        
        for(i=0;i<threads;i++)
        {
            pthread_join( thread_local[i], NULL);
        }
        
  //  }
    
    
    //  free(CrawlArgumentStruct);
    //free(P1);
    quicksort(sorted_index,mark, 0, N-1);
    
    /*
     for (j=0; j<threads-1; j++) {
     printf("Creating thread %d \n",j);
     SortArgumentStruct[j].left = j*nodes_per_thread;
     SortArgumentStruct[j].mark_array = mark;
     SortArgumentStruct[j].right = j*nodes_per_thread+nodes_per_thread-1;
     SortArgumentStruct[j].sort_index_array = sorted_index;
     SortArgumentStruct[j].tid = j;
     printf("left = %d \t right = %d \n ",j*nodes_per_thread,j*nodes_per_thread+nodes_per_thread-1 );
     pthread_create(&thread_local1[j], NULL, quicksort_threaded, (void*)&SortArgumentStruct[j]);
     }
     
     SortArgumentStruct[j].left = j*nodes_per_thread;
     SortArgumentStruct[j].mark_array = mark;
     SortArgumentStruct[j].right = j*nodes_per_thread+rem-1;
     SortArgumentStruct[j].sort_index_array = sorted_index;
     SortArgumentStruct[j].tid = j;
         printf("left = %d \t right = %d \n ",j*nodes_per_thread,j*nodes_per_thread+nodes_per_thread+rem-1);
     
     pthread_create(&thread_local1[j], NULL, quicksort_threaded, (void*)&SortArgumentStruct[j]);
     
     for(i=0;i<threads;i++)
     {
     pthread_join( thread_local1[i], NULL);
     }
     */
    gettimeofday(&finish_time1, NULL);
    
    elapsed = ((double)(finish_time1.tv_usec - start_time1.tv_usec)/1000000.0)+((double)(finish_time1.tv_sec - start_time1.tv_sec));
    
    printf("Time for parallel execution = %lf \n",elapsed);
    
    printf("Top 100 Nodes in descending order \n ");
    for(i=N-1;i>N-101;i--)
        printf("index = %d \t, mark = %d \n", sorted_index[i],mark[sorted_index[i]]);
    
   
    

    
    //printf("Parallel Time = %lf \n DiffSec = %ld \n DiffuSec = %d",elapsed,finish_time1.tv_sec-start_time1.tv_sec,finish_time1.tv_usec-start_time1.tv_usec);
#endif
    
    
    
    exit(0);
    
    
}

void *print_message_function( void *ptr )
{
    char *message;
    message = (char *) ptr;
    printf("%s \n", message);
}

void *crawl_task(void *ptr)
{
    
    int r;
    int i,k;
    int *mark;
    int *crawler;
    struct node_element *node_info;
    float **DataBlock;
    int NumOfNodes;
    struct CrawlThreadArgument *myArgument =  (struct CrawlThreadArgument*)ptr;
    int crawl_index;
    struct node_element *mynodeelement;
    
    
    mark = myArgument->mark;
    crawler = myArgument->crawler;
    node_info = myArgument->node_info;
    DataBlock = myArgument->DataBlock;
    NumOfNodes = myArgument->NumOfNodes;
    pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
    
    
     for(i=0;i<NumberOfSteps;i++)
     {
    for(k=0;k<NumOfNodes;k++)
    {
        
        crawl_index = crawler[k];
        r = rand() % node_info[crawl_index].num_of_nodes;
        // r = (int)(drand48()*(node_info[crawler[k]].num_of_nodes));
        
        pthread_mutex_lock( &mutex1 );
        mark[crawl_index]++;
        //DataBlock[crawl_index][3]++;
        crawler[k] = (int)DataBlock[node_info[crawl_index].index+r][0];
        pthread_mutex_unlock( &mutex1 );
        
    }
         log_barrier_function(my_log_barrier,myArgument->tid,myArgument->num_of_threads);

     }
       pthread_exit(NULL);
}



int partition(int* sort_index_array,int* DataBlock, int left, int right, int pivotIndex)
{
    
    //   printf("Entered Partition \n");
    
    int pivotValue = sort_index_array[pivotIndex];
    int dummy;
    int i;
    int storedIndex = left;
    
    //   printf("Entered Succesffully \n");
    dummy = sort_index_array[pivotIndex];
    sort_index_array[pivotIndex] = sort_index_array[right];
    sort_index_array[right] = dummy;
    
    //   printf("First Exchange Done \n");
    
    
    for (i=left; i<right; i++) {
        
        
        if(DataBlock[sort_index_array[i]]<DataBlock[pivotValue])
        {
            dummy = sort_index_array[i];
            sort_index_array[i] = sort_index_array[storedIndex];
            sort_index_array[storedIndex] = dummy;
            storedIndex++;
        }
        
    }
    
    dummy = sort_index_array[storedIndex];
    sort_index_array[storedIndex] = sort_index_array[right];
    sort_index_array[right] = dummy;
    
    //  printf("Exit Partition \n");
    
    
    return storedIndex;
    
}

void *quicksort(int *sort_index_array,int* mark_array,int left,int right)
{
    int pivotIndex;
    int pivotNewIndex;
    // If the list has 2 or more items
    if (left < right)
    {
        //choose any pivotIndex such that left ≤ pivotIndex ≤ right
        pivotIndex = (left+right)/2;
        // Get lists of bigger and smaller items and final position of pivot
        //    printf("pivotNewIndex = %d \n Left = %d \t Right = %d \t Left Sort \n",pivotNewIndex, left, right);
        
        
        ///pivotNewIndex = partition(sort_index_array, mark_array,left, right, pivotIndex);
        
        int pivotValue = sort_index_array[pivotIndex];
        int dummy;
        int i;
        int storedIndex = left;
        
        dummy = sort_index_array[pivotIndex];
        sort_index_array[pivotIndex] = sort_index_array[right];
        sort_index_array[right] = dummy;
        
        for (i=left; i<right; i++) {
            
            
            if(mark_array[sort_index_array[i]]<mark_array[pivotValue])
            {
                dummy = sort_index_array[i];
                sort_index_array[i] = sort_index_array[storedIndex];
                sort_index_array[storedIndex] = dummy;
                storedIndex++;
            }
            
        }
        
        dummy = sort_index_array[storedIndex];
        sort_index_array[storedIndex] = sort_index_array[right];
        sort_index_array[right] = dummy;
        
        
        pivotNewIndex = storedIndex;
        
        
        
        // Recursively sort elements smaller than the pivo
        
        quicksort(sort_index_array, mark_array,left, pivotNewIndex - 1);
        
        
        //   printf("Right Sort \n");
        
        // Recursively sort elements at least as big as the pivot
        quicksort(sort_index_array, mark_array,pivotNewIndex + 1, right);
    }
    
}



void *quicksort_threaded(void *ptr)
{
    struct SortThreadArgument *myarguments = (struct SortThreadArgument*)ptr;
    int *sort_index_array = myarguments->sort_index_array;
    int *mark_array = myarguments->mark_array;
    int left = myarguments->left;
    int right = myarguments->right;
    
    quicksort(sort_index_array, mark_array,left, right);
    
    
    printf("Thread %d done \n", myarguments->tid);
    
    pthread_exit(NULL);
    
}


void heapSort(int* a,int *DataArray, int count){
    int end;
    //(first place a in max-heap order)
    heapify(a, DataArray, count);
    
    //in languages with zero-based arrays the children are 2*i+1 and 2*i+2
    end = count-1;
    
    do{
        //(swap the root(maximum value) of the heap with the last element of the heap)
        swap(&a[end], &a[0]);
        // (decrease the size of the heap by one so that the previous max value will
        //  stay in its proper placement)
        end--;
        //(put the heap back in max-heap order)
        siftDown(a,DataArray, 0, end);
    }while (end > 0);
}

void heapify(int* a,int *DataArray,int count)
{
    //(start is assigned the index in a of the last parent node)
    int start = (count - 2)/2 ;
    
    do{
        // (sift down the node at index start to the proper place such that all nodes below
        //  the start index are in heap order)
        siftDown(a, DataArray,start, count-1);
        start = start - 1 ;
        //(after sifting down the root all nodes/elements are in heap order)
    }while(start>=0);
}

void swap(int *left,int *right)
{
    int dummy;
    dummy = *left;
    *left = *right;
    *right = dummy;
}

void siftDown(int *a, int *DataArray,int start,int end)
{
    int root = a[start];
    int child;
    int swapper;
    
    do{
        child = root * 2 + 1 ; //      (root*2 + 1 points to the left child)
        swapper = root;        //(keeps track of child to swap with)
        // (check if root is smaller than left child)
        if(DataArray[a[swapper]]<DataArray[a[child]])
            swapper = child;
        // (check if right child exists, and if it's bigger than what we're currently swapping with)
        if(((child+1)<=end) && (DataArray[a[swapper]]<DataArray[a[child+1]]))
            swapper = child+1;
        //(check if we need to swap at all)
        if(a[swapper]!=a[root]){
            swap(&a[root], &a[swapper]);
            root = swapper;
        }//          (repeat to continue sifting down the child now)
        else
            return;
    }while(((root * 2) + 1)<=end);
    
    //(While the root has at least one child)
    
}

