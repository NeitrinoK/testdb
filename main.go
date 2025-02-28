package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"scylladb_example/object"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/sirupsen/logrus"

	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

var Logger = initLogger()

func initLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.Formatter = &logrus.TextFormatter{FullTimestamp: true}
	gocql.Logger = logger
	log.SetOutput(logger.Writer())
	return logger
}

// Function to generate a random string of a specified length
func generateRandomString(length int) string {
	characters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = characters[rand.Intn(len(characters))]
	}
	return string(result)
}

var (
	ClusterNodes                = []string{"172.18.0.3", "172.18.0.4", "172.18.0.5", "172.18.0.6", "172.18.0.2"}
	TotalObjectsDefaults        = 5000000
	NumBucketsDefaults          = 2000
	NumBuckets                  = 0
	NumObjects                  = 0
	NumVersions                 = 10
	TableName            string = "metadata"
	TableNameBlob        string = "metadata_blob"
	KeySpace             string = "s3"
)

func executeWithRetry(session *gocql.Session, insertQuery, bucketName, objectName, version string, owner_display_name string, owner_id string, maxRetries int, retryDelay time.Duration) error {
	var err error
	tenant := "sb"
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Execute the query
		err = session.Query(insertQuery, tenant, bucketName, objectName, version, time.Now(), owner_display_name, owner_id).Exec()

		// If the query succeeds, exit the loop
		if err == nil {
			return nil
		}

		// Log the error and retry after a delay
		if attempt > 1 {
			Logger.Warnf("Attempt %d failed: %v", attempt+1, err)
		}
		if attempt < maxRetries {
			time.Sleep(retryDelay)
		}

	}

	// If all retries fail, return the last error
	return fmt.Errorf("failed to execute query after %d attempts: %w", maxRetries+1, err)
}

func executeWithRetryBlob(session *gocql.Session, insertQuery, bucketName, objectName, version string, owner_display_name string, owner_id string, maxRetries int, retryDelay time.Duration) error {
	var err error
	msg := &object.ObjectMetadata{
		Tenant:           "sb",
		Bucket:           bucketName,
		Object:           objectName,
		VersionId:        version,
		LastModified:     timestamppb.Now(),
		OwnerDisplayName: owner_display_name,
		OwnerId:          owner_id,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Execute the query
		err = session.Query(insertQuery, objectName, data).Exec()

		// If the query succeeds, exit the loop
		if err == nil {
			return nil
		}

		// Log the error and retry after a delay
		if attempt > 1 {
			Logger.Warnf("Attempt %d failed: %v", attempt+1, err)
		}
		if attempt < maxRetries {
			time.Sleep(retryDelay)
		}

	}

	// If all retries fail, return the last error
	return fmt.Errorf("failed to execute query after %d attempts: %w", maxRetries+1, err)
}

// Function that will be executed by each goroutine
func worker(numObjects int, numVersions int, progressChannel chan float64, wg *sync.WaitGroup, session *gocql.Session) error {
	defer wg.Done() // Decrement the WaitGroup counter when the goroutine completes
	// Logger.Printf("Goroutine %d starting work\n", id)

	bucketName := "bucket-" + generateRandomString(8)

	// tenant TEXT,
	// bucket TEXT,
	// object TEXT,
	// version_id TEXT,
	// last_modified TIMESTAMP,
	// owner_display_name TEXT,
	// owner_id TEXT,

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (tenant, bucket, object, version_id, last_modified, owner_display_name, owner_id) VALUES (?, ?, ?, ?, ?, ?, ?)`, TableName)

	owner_display_name := generateRandomString(8)
	owner_id := generateRandomString(8)
	maxRetries := 10
	retryDelay := 1000 * time.Millisecond
	for i := 0; i < numObjects; i++ {
		objectName := "object-" + generateRandomString(8)
		for j := 0; j < numVersions; j++ {
			version := "version-" + generateRandomString(8)
			if err := executeWithRetry(session, insertQuery, bucketName, objectName, version, owner_display_name, owner_id, maxRetries, retryDelay); err != nil {
				Logger.Fatalf("Could not insert data: %v", err)
			}
		}
		progressChannel <- 1.0 / float64(numObjects) / float64(NumBuckets)
	}

	// Logger.Printf("Goroutine %d finished work\n", id)
	return nil
}

func blob_worker(numObjects int, numVersions int, progressChannel chan float64, wg *sync.WaitGroup, session *gocql.Session) error {
	defer wg.Done() // Decrement the WaitGroup counter when the goroutine completes
	// Logger.Printf("Goroutine %d starting work\n", id)

	bucketName := "bucket-" + generateRandomString(8)

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (object, data) VALUES (?, ?)`, TableNameBlob)

	owner_display_name := generateRandomString(8)
	owner_id := generateRandomString(8)
	maxRetries := 10
	retryDelay := 1000 * time.Millisecond
	for i := 0; i < numObjects; i++ {
		objectName := "object-" + generateRandomString(8)
		for j := 0; j < numVersions; j++ {
			version := "version-" + generateRandomString(8)
			if err := executeWithRetryBlob(session, insertQuery, bucketName, objectName, version, owner_display_name, owner_id, maxRetries, retryDelay); err != nil {
				Logger.Fatalf("Could not insert data: %v", err)
			}
		}
		progressChannel <- 1.0 / float64(numObjects) / float64(NumBuckets)
	}

	// Logger.Printf("Goroutine %d finished work\n", id)
	return nil
}

func createTableQuery(tableName string) string {
	createTableQuery := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (`+`
	tenant TEXT,
	bucket TEXT,
	object TEXT,
    version_id TEXT,
	last_modified TIMESTAMP,
    owner_display_name TEXT,
    owner_id TEXT,
	PRIMARY KEY ((bucket, object, version_id), last_modified)
    );`, tableName)
	return createTableQuery
}

func createTableQueryBlob(tableName string) string {
	createTableQuery := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (`+`
	object TEXT,
    data blob,
	PRIMARY KEY (object)
    );`, tableName)
	return createTableQuery
}

func createKeyspace(session *gocql.Session, keySpace string) error {
	createKeyspaceQuery := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
			'class' : 'SimpleStrategy',
			'replication_factor' : 3
		}`, keySpace)
	err := session.Query(createKeyspaceQuery).Exec()
	if err != nil {
		log.Fatalf("Could not create keyspace: %v", err)
		return err
	}
	Logger.Infof("Keyspace '%s' created successfully.\n", keySpace)
	return nil
}

func createTable(session *gocql.Session, tableName string) error {
	createTableQuery := createTableQuery(tableName)
	Logger.Infof("Table '%s' created successfully.\n", tableName)
	Logger.Infof("Query: \n '%s' .\n", createTableQuery)
	err := session.Query(createTableQuery).Exec()
	if err != nil {
		Logger.Fatalf("Could not create table: %v", err)
		return err
	}
	Logger.Infof("Table '%s' created successfully.\n", tableName)
	return nil
}

func createTableBlob(session *gocql.Session, tableName string) error {
	createTableQuery := createTableQueryBlob(tableName)
	Logger.Infof("Table '%s' created successfully.\n", tableName)
	Logger.Infof("Query: \n '%s' .\n", createTableQuery)
	err := session.Query(createTableQuery).Exec()
	if err != nil {
		Logger.Fatalf("Could not create table: %v", err)
		return err
	}
	Logger.Infof("Table '%s' created successfully.\n", tableName)
	return nil
}

func dropTable(session *gocql.Session, tableName string) error {
	dropTableQuery := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName)
	if err := session.Query(dropTableQuery).Exec(); err != nil {
		log.Fatalf("Could not drop table: %v", err)
		return err
	}
	Logger.Infof("Table '%s' dropped successfully.\n", tableName)
	return nil
}

func dropKeyspace(session *gocql.Session, keySpace string) error {
	dropKeyspaceQuery := fmt.Sprintf(`DROP KEYSPACE IF EXISTS %s`, keySpace)
	if err := session.Query(dropKeyspaceQuery).Exec(); err != nil {
		log.Fatalf("Could not drop keyspace: %v", err)
		return err
	}
	Logger.Infof("Keyspace '%s' dropped successfully.\n", keySpace)
	return nil

}

func createCluster() *gocql.ClusterConfig {
	cluster := gocql.NewCluster(ClusterNodes...)
	// cluster.ReconnectInterval = 10 * time.Millisecond
	// cluster.Timeout = 100 * time.Millisecond
	// cluster.ConnectTimeout = 100 * time.Millisecond
	cluster.Keyspace = "system"
	cluster.Consistency = gocql.Quorum
	cluster.DisableInitialHostLookup = true
	// cluster.ReconnectionPolicy = &gocql.ConstantReconnectionPolicy{MaxRetries: 10, Interval: 8 * time.Millisecond}
	// cluster.MaxWaitSchemaAgreement = 100 * time.Millisecond
	return cluster
}

// func selectAllData(session *gocql.Session, tableName string) error {
// 	selectQuery := fmt.Sprintf(`SELECT count(*) FROM %s`, tableName)
// 	iter := session.Query(selectQuery).Iter()
// 	Logger.Infof("Data in the table:")
// 	var count int
// 	if iter.Scan(&count) {
// 		Logger.Infof("Count: %d\n", count)
// 	} else {
// 		Logger.Errorf("Failed to scan count: %v\n", iter.Close())
// 	}
// 	return nil
// }

func testColumn(session *gocql.Session) {
	if err := createTable(session, TableName); err != nil {
		log.Fatalf("Could not create table: %v", err)
		return
	}

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	progressChannel := make(chan float64)

	startTime := time.Now()
	// Start numGoroutines goroutines
	for i := 0; i < NumBuckets; i++ {
		wg.Add(1) // Increment the WaitGroup counter
		go worker(NumObjects, NumVersions, progressChannel, &wg, session)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop() // Ensure the ticker is stopped when we're done

	go func() {
		completed := 0.0
		wasCompleted := 0.0
		wasDuration := 0.0

		for {
			select {
			case p, ok := <-progressChannel:
				if !ok {
					return // Exit if the channel is closed
				}
				completed += p
			case <-ticker.C:
				{ // Every second, print progress
					duration := time.Since(startTime).Seconds()
					tick := duration - wasDuration
					koef := float64(NumBuckets * NumObjects * NumVersions)
					operationsPerSecondAvg := completed * koef / duration
					operationsPerSecond := (completed - wasCompleted) * koef / tick
					remaining := time.Duration((1.0 - completed) * koef / (operationsPerSecondAvg + 0.00000000001) * float64(time.Second))
					fmt.Printf("Progress: %.2f%% Speed: %f objects/second. Average Speed: %f Time remaining: %s \n", completed*100, operationsPerSecond, operationsPerSecondAvg, remaining.String())
					wasDuration = duration
					wasCompleted = completed
				}
			}
		}
	}()

	// Wait for all goroutines to finish
	wg.Wait()
	close(progressChannel)
	duration := time.Since(startTime).Seconds()
	Logger.Infof("Inserted %d objects in %f seconds.\n", NumBuckets*NumObjects*NumVersions, duration)
	Logger.Infof("Speed is %f objects/second.\n", float64(NumBuckets*NumObjects*NumVersions)/duration)

	Logger.Infof("Data inserted successfully.")
	// time.Sleep(1 * time.Second)
	// selectAllData(session, TableName)
	dropTable(session, TableName)

}

func testBlob(session *gocql.Session) {
	if err := createTableBlob(session, TableNameBlob); err != nil {
		log.Fatalf("Could not create table: %v", err)
		return
	}

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	progressChannel := make(chan float64)

	startTime := time.Now()
	// Start numGoroutines goroutines
	for i := 0; i < NumBuckets; i++ {
		wg.Add(1) // Increment the WaitGroup counter
		go blob_worker(NumObjects, NumVersions, progressChannel, &wg, session)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop() // Ensure the ticker is stopped when we're done

	go func() {
		completed := 0.0
		wasCompleted := 0.0
		wasDuration := 0.0

		for {
			select {
			case p, ok := <-progressChannel:
				if !ok {
					return // Exit if the channel is closed
				}
				completed += p
			case <-ticker.C:
				{ // Every second, print progress
					duration := time.Since(startTime).Seconds()
					tick := duration - wasDuration
					koef := float64(NumBuckets * NumObjects * NumVersions)
					operationsPerSecondAvg := completed * koef / duration
					operationsPerSecond := (completed - wasCompleted) * koef / tick
					remaining := time.Duration((1.0 - completed) * koef / (operationsPerSecondAvg + 0.00000000001) * float64(time.Second))
					fmt.Printf("Progress: %.2f%% Speed: %f objects/second. Average Speed: %f Time remaining: %s \n", completed*100, operationsPerSecond, operationsPerSecondAvg, remaining.String())
					wasDuration = duration
					wasCompleted = completed
				}
			}
		}
	}()

	// Wait for all goroutines to finish
	wg.Wait()
	close(progressChannel)
	duration := time.Since(startTime).Seconds()
	Logger.Infof("Inserted %d objects in %f seconds.\n", NumBuckets*NumObjects*NumVersions, duration)
	Logger.Infof("Speed is %f objects/second.\n", float64(NumBuckets*NumObjects*NumVersions)/duration)

	Logger.Infof("Data inserted successfully.")

	// selectAllData(session, TableName)
	dropTable(session, TableNameBlob)

}

func main() {
	Logger.Printf("The application was built with the Go version: %s\n", runtime.Version())
	threadCountPtr := flag.Int("thread", NumBucketsDefaults, "Port number to listen on")
	totalObjectsPtr := flag.Int("num", TotalObjectsDefaults, "Number of objects to insert")
	modePtr := flag.Bool("blob-mode", false, "Enable blob mode")
	flag.Parse()
	threadCount := *threadCountPtr
	totalObjects := *totalObjectsPtr
	mode := *modePtr

	f, err := os.Create("cpu.prof")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	NumBuckets = threadCount
	NumObjects = totalObjects / NumBuckets

	cluster := createCluster()

	initSession, err := cluster.CreateSession()
	if err != nil {
		log.Panicf("Could not connect to ScyllaDB: %v", err)
	}
	defer initSession.Close()

	if err := createKeyspace(initSession, KeySpace); err != nil {
		Logger.Errorf("Could not create keyspace: %v", err)
		os.Exit(2)
	}

	cluster.Keyspace = KeySpace
	session, err := cluster.CreateSession()
	if err != nil {
		log.Panicf("Could not connect to ScyllaDB: %v", err)
	}
	defer session.Close()

	if mode {
		log.Print("Blob mode")
		testBlob(session)
	} else {
		log.Print("Column mode")
		testColumn(session)
	}

	dropKeyspace(session, KeySpace)

}
