#include <stdio.h>
#include <stdlib.h>



/* Code to run before enqueueing HistJobs */
int hist_size = 1;
for (i = 0; i < n_lsb; ++i)
  hist_size *= 2;
int thread_num = 10;
int **histograms = malloc(thread_num * sizeof(int *));
for (size_t i = 0; i < thread_num; i++)
  histograms[i] = NULL;
//Split up the num_tuples so each thread gets the same
int tuples_per_thread = num_tuples/thread_num;
//For each thread
for (size_t i = 0; i < thread_num; i++) {
  //Set the arguments
  hist_arguments *arguments = malloc(sizeof(hist_arguments));
  arguments->hist = &histograms[i];
  arguments->n_lsb = n_lsb;
  arguments->hist_size = hist_size;
  arguments->rel = relation;
  arguments->start = i * tuples_per_thread;
  //If we are setting up the last thread, end is the end of the relation
  if(i != thread_num - 1)arguments->end = (i+1) * tuples_per_thread;
  else arguments->end = num_tuples;
  //Enqueue it in the scheduler
}
/* End of code to run before enqueueing hist_jobs */

void* HistJob(void *arguments)
{
  hist_arguments *args = arguments;

  //Allocate memory for the thread's histogram and set each value to 0.
  args->(*hist) = calloc(args->hist_size, sizeof(int));

  for (size_t i = args->start; i < args->end; i++) {
    uint64_t hashed_value = HashFunction1(args->rel[i], args->n_lsb);
    args->(*hist)[hashed_value]++;
  }
  return;
}

/* Code to run after HistJobs and before PartitionJobs */

//Wait for all threads to finish building their work
//Allocate the Hist and Psum arrays
int *Psum = malloc(hist_size * sizeof(int));
int *Hist = calloc(hist_size, sizeof(int));
for (size_t i = 0; i < hist_size; i++)
  Psum[i] = -1;

//Build the whole Histogram from the
for (size_t i = 0; i < thread_num; i++)
  for (size_t j = 0; j < hist_size; j++)
    Hist[j] += histograms[i][j];

//Free allocated space
for (size_t i = 0; i < thread_num; i++)
  free(histograms[i]);
free(histograms);

//Build the Psum array
int new_start = 0;
for (i = 0; i < hist_size; ++i)
{
  /*If the current bucket has 0 values allocated to it then leave
   *psum[CurrentBucket][1] to -1.	*/
  if (Hist[i] > 0)
  {
    Psum[i] = new_start;
    temp_psum[i] = new_start;
    new_start += Hist[i];
  }
}
