package utils

import (
	"context"
	"embed"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hkloudou/lake/internal/xsync"
	"github.com/redis/go-redis/v9"
)

//go:embed lua/*.lua
var luaFS embed.FS

var (
	scriptHashes   map[string]string // filename -> Redis SHA
	scriptHashesMu sync.RWMutex
	fight          xsync.SingleFlight[any] // for loading all scripts
)

func init() {
	scriptHashes = make(map[string]string)
	fight = xsync.NewSingleFlight[any]()
}

// loadScriptsToRedis loads all .lua files from embedded FS to Redis
// Uses fight.Do to ensure only one execution in multi-threaded environment
func loadScriptsToRedis(rdb *redis.Client) error {
	_, err := fight.Do("loadScriptsToRedis", func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Read all lua files from embedded directory
		entries, err := luaFS.ReadDir("lua")
		if err != nil {
			return nil, fmt.Errorf("failed to read lua directory: %w", err)
		}

		scriptHashesMu.Lock()
		defer scriptHashesMu.Unlock()

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			filename := entry.Name()
			if filepath.Ext(filename) != ".lua" {
				continue
			}

			// Read script content
			content, err := luaFS.ReadFile(filepath.Join("lua", filename))
			if err != nil {
				return nil, fmt.Errorf("failed to read script %s: %w", filename, err)
			}
			// Load script to Redis
			sha, err := rdb.ScriptLoad(ctx, string(content)).Result()
			if err != nil {
				return nil, fmt.Errorf("failed to load script %s to Redis: %w", filename, err)
			}

			// Store filename -> Redis SHA mapping
			scriptHashes[filename] = sha
			log.Printf("loaded script %s to Redis: %s\n", filename, sha)
		}

		return nil, nil
	})
	return err
}

// getScriptHash returns the Redis SHA hash for a given script filename
// Returns empty string if script not found in cache
func getScriptHash(filename string) string {
	scriptHashesMu.RLock()
	defer scriptHashesMu.RUnlock()
	return scriptHashes[filename] // safe: returns "" if key doesn't exist
}

// ensureScriptLoaded ensures the script is loaded and returns its SHA
// If force=true, reload scripts even if SHA exists in cache (e.g., after Redis restart)
func ensureScriptLoaded(redisClient *redis.Client, filename string, force bool) (string, error) {
	// Check cache first (unless forced)
	if !force {
		if sha := getScriptHash(filename); sha != "" {
			return sha, nil
		}
	}

	// Load all scripts to Redis (protected by SingleFlight, updates entire hash table)
	if err := loadScriptsToRedis(redisClient); err != nil {
		return "", err
	}

	// Get SHA from updated cache
	sha := getScriptHash(filename)
	if sha == "" {
		return "", fmt.Errorf("script %s not found", filename)
	}

	return sha, nil
}

func SafeEvalSha(ctx context.Context, redisClient *redis.Client, filename string, keys []string, args ...interface{}) *redis.Cmd {
	// Get SHA (load if not cached)
	sha, err := ensureScriptLoaded(redisClient, filename, false)
	if err != nil {
		return redis.NewCmd(ctx, err)
	}

	// Execute script
	cmd := redisClient.EvalSha(ctx, sha, keys, args...)

	// Handle NOSCRIPT: Redis restarted, force reload all scripts
	if cmd.Err() != nil && strings.Contains(cmd.Err().Error(), "NOSCRIPT") {
		sha, err = ensureScriptLoaded(redisClient, filename, true)
		if err != nil {
			return redis.NewCmd(ctx, err)
		}
		cmd = redisClient.EvalSha(ctx, sha, keys, args...)
	}

	return cmd
}
