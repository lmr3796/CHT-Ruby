CHT-Ruby
=======
procedure of a job

1. Client submits job to dispatcher.
2. The dispatcher returns the id of the job.
3. Client asks dispatcher for a worker
4. The dispatcher returns the id of a worker.
   The thread would be blocked in dispatcher until a worker is found.
5. Client spawns a thread to send the request to the worker.
   If there is any remaining tasks, the main thread goes to 3; otherwise, it goes to 6.
6. Client tells the dispatcher that the job is done.

------
procedure of a worker

1. Tell the status checker that it's idle
2. Wait for a task to do.
3. Go to 1 when task done.

------
rescheduling

1. when a job is done: assign the workers to other jobs if necessary
2. when a job is submitted: find idle workers for the job; if none, get some workers from other job (after their current task is done).

