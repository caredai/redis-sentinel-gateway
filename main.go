package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"redis-sentinel-failover/kube"
	"redis-sentinel-failover/sentinel"

	"github.com/redis/go-redis/v9"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// getEnv returns the value of the environment variable if set, or a fallback value otherwise
func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

// getNamespace reads the namespace in which the Pod is running from the Kubernetes service account secret
func getNamespace() string {
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Warning: Namespace file does not exist, using default namespace")
			return "default"
		} else {
			fmt.Printf("Error reading namespace file: %v\n", err)
			os.Exit(1)
		}
	}
	return string(data)
}

func main() {
	// Getting configuration from environment variables
	serviceName := getEnv("SERVICE_NAME", "redis-failover")
	sentinelAddr := getEnv("SENTINEL_ADDR", "rfs-redis-node:26379")
	masterName := getEnv("MASTER_NAME", "mymaster")
	namespace := getEnv("NAMESPACE", getNamespace())
	leaseName := getEnv("LEASE_NAME", "redis-failover-lease")
	portName := getEnv("PORT_NAME", "redis")
	portNumberStr := getEnv("PORT_NUMBER", "6379")
	tickIntervalStr := getEnv("TICK_INTERVAL", "5s")
	leaderID := os.Getenv("HOSTNAME")
	password := os.Getenv("REDIS_PASSWORD")

	// Convert tickInterval and portNumber from string to appropriate types
	tickInterval, err := time.ParseDuration(tickIntervalStr)
	if err != nil {
		panic(fmt.Sprintf("Invalid tick interval: %v", err))
	}

	portNumber, err := strconv.Atoi(portNumberStr)
	if err != nil {
		panic(fmt.Sprintf("Invalid port number: %v", err))
	}

	// Context and signal handling
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	clientset, err := kube.GetKubernetesClient()
	if err != nil {
		panic(err.Error())
	}

	// Create Redis Sentinel client
	sentinelClient := redis.NewSentinelClient(&redis.Options{
		Addr:     sentinelAddr,
		Password: password,
	})

	err = kube.CreateKubeService(ctx, clientset, namespace, serviceName, portName, int32(portNumber))
	if err != nil {
		fmt.Printf("Error creating service: %v\n", err)
	}

	// Configure leader election via Kubernetes Lease
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: leaderID,
		},
	}

	// Liveness: fails if we hold the lease but stopped renewing it for longer than the timeout.
	healthz := leaderelection.NewLeaderHealthzAdaptor(20 * time.Second)
	go serveHealthz(getEnv("HEALTH_ADDR", ":8080"), healthz)

	// RunOrDie returns as soon as leadership is lost. Keep re-entering the election
	// until we are asked to shut down, so a transient API/etcd outage can't leave
	// the process alive but idle (and the Endpoints stale) forever.
	for ctx.Err() == nil {
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			LeaseDuration:   15 * time.Second,
			RenewDeadline:   10 * time.Second,
			RetryPeriod:     2 * time.Second,
			ReleaseOnCancel: true,
			WatchDog:        healthz,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					fmt.Println("Became the leader, starting to manage Endpoints")
					runEndpointUpdater(ctx, clientset, sentinelClient, namespace, serviceName, masterName, portName, int32(portNumber), tickInterval)
				},
				OnStoppedLeading: func() {
					fmt.Println("Lost leadership, stopping Endpoints management")
				},
			},
		})
		if ctx.Err() == nil {
			fmt.Println("Leader election ended, re-entering election")
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
			}
		}
	}
}

func serveHealthz(addr string, healthz *leaderelection.HealthzAdaptor) {
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := healthz.Check(r); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("ok"))
	})
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Printf("Health server error: %v\n", err)
		os.Exit(1)
	}
}

func runEndpointUpdater(ctx context.Context, clientset *kubernetes.Clientset, sentinelClient *redis.SentinelClient, namespace, serviceName, masterName, portName string, portNumber int32, tickInterval time.Duration) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			masterIP, err := sentinel.GetMasterIP(ctx, sentinelClient, masterName)
			if err != nil {
				fmt.Printf("Error getting master IP: %v\n", err)
				continue
			}

			// Compare with what is actually in the cluster, not a cached value,
			// so drift (manual edits, a previous leader's write) is corrected.
			currentIP, err := kube.GetKubeEndpointIP(ctx, clientset, namespace, serviceName)
			if err != nil {
				fmt.Printf("Error reading Kubernetes Endpoint: %v\n", err)
				continue
			}
			if masterIP == currentIP {
				continue
			}

			err = kube.UpdateKubeEndpoint(ctx, clientset, namespace, serviceName, masterIP, portName, portNumber)
			if err != nil {
				fmt.Printf("Error updating Kubernetes Endpoint: %v\n", err)
			} else {
				fmt.Printf("Updated Kubernetes Endpoint with new master IP: %s (was %q)\n", masterIP, currentIP)
			}
		case <-ctx.Done():
			fmt.Println("Stopping endpoint updater")
			return
		}
	}
}
