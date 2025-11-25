package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

// Task model
type Task struct {
	ID    int
	Input int
}

// ResultsStore for shared output (list + file)
type ResultsStore struct {
	mu      sync.Mutex
	results []string
	writer  *bufio.Writer
	file    *os.File
}

func NewResultsStore(path string) (*ResultsStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &ResultsStore{
		results: make([]string, 0),
		writer:  bufio.NewWriter(f),
		file:    f,
	}, nil
}

func (rs *ResultsStore) Add(result string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.results = append(rs.results, result)
	if _, err := rs.writer.WriteString(result + "\n"); err != nil {
		log("ERROR writing to file:", err)
		return
	}
	rs.writer.Flush()
}

func (rs *ResultsStore) Snapshot() []string {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	cp := make([]string, len(rs.results))
	copy(cp, rs.results)
	return cp
}

func (rs *ResultsStore) Close() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.writer.Flush()
	rs.file.Close()
}

// Worker goroutine
func worker(workerID int, tasks <-chan Task, rs *ResultsStore, wg *sync.WaitGroup) {
	defer wg.Done()
	log(fmt.Sprintf("Worker-%d started.", workerID))

	for task := range tasks {
		output := processTask(task)

		line := fmt.Sprintf(
			"Worker-%d processed Task-%d (input=%d) -> output=%d",
			workerID, task.ID, task.Input, output,
		)
		rs.Add(line)
		log(line)
	}

	log(fmt.Sprintf("Worker-%d completed.", workerID))
}

func processTask(task Task) int {
	// simulate time
	time.Sleep(time.Millisecond * time.Duration(150+(task.Input%5)*50))
	return task.Input*task.Input + task.ID
}

func main() {
	const NUM_TASKS = 20
	const NUM_WORKERS = 4
	const OUTPUT_FILE = "go_results.txt"

	rs, err := NewResultsStore(OUTPUT_FILE)
	if err != nil {
		log("FATAL file I/O error:", err)
		return
	}
	defer rs.Close()

	// Channel is concurrency-safe queue
	tasks := make(chan Task, NUM_TASKS)

	// Start workers
	var wg sync.WaitGroup
	wg.Add(NUM_WORKERS)
	for w := 1; w <= NUM_WORKERS; w++ {
		go worker(w, tasks, rs, &wg)
	}

	// Enqueue tasks
	for i := 1; i <= NUM_TASKS; i++ {
		tasks <- Task{ID: i, Input: i * 3}
	}
	log(fmt.Sprintf("Enqueued %d tasks.", NUM_TASKS))

	close(tasks) // signal no more tasks
	wg.Wait()

	log(fmt.Sprintf("All workers finished. Total results = %d", len(rs.Snapshot())))
}

func log(v ...any) {
	fmt.Println(append([]any{fmt.Sprintf("[%s]", time.Now().Format("15:04:05.000"))}, v...)...)
}
