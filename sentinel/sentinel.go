package sentinel

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// GetMasterIP retrieves the IP address of the Redis master from Sentinel using the provided Sentinel client.
func GetMasterIP(ctx context.Context, sentinel *redis.SentinelClient, masterName string) (string, error) {
	addr, err := sentinel.GetMasterAddrByName(ctx, masterName).Result()
	if err != nil {
		return "", err
	}

	if len(addr) == 0 {
		return "", fmt.Errorf("sentinel returned no address for master %q", masterName)
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupIP(lookupCtx, "ip4", addr[0])
	if err != nil {
		return "", err
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("no IPv4 address for master host %q", addr[0])
	}
	return ips[0].String(), nil
}
