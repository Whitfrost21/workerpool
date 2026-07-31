# workerpool 
workerpool written in golang for batch processing.

It currently includes - 
1. concurrent job execution.
2. shutdown sequencing (jobs close -> worker's drain -> results close -> drainer finishes).
3. deduplication via cache and inflight claiming(race free,no mutexes).
4. pool level cancellation (via context).

On way to arrive -
- panic safety while execution of jobs 
- guarding submissions after pool shutdown 
- error surfacing 
- tests 
- pool tuning for flexibility
