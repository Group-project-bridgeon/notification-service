package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func findSeedsSQL() (string, error) {
	possiblePaths := []string{
		"seeds/seeds.sql",
		"../seeds/seeds.sql",
		"../../seeds/seeds.sql",
		filepath.Join(".", "seeds", "seeds.sql"),
	}

	for _, p := range possiblePaths {
		if data, err := os.ReadFile(p); err == nil {
			return string(data), nil
		}
	}

	return "", fmt.Errorf("could not find seeds/seeds.sql in relative paths")
}

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://notification_user:notification_pass@localhost:5432/notification_db?sslmode=disable"
	}

	seedsSQL, err := findSeedsSQL()
	if err != nil {
		log.Fatalf("[SEED] Failed to load seeds file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("[SEED] Connecting to database...")
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("[SEED] Failed to connect to database: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("[SEED] Database ping failed: %v", err)
	}

	fmt.Println("[SEED] Applying seeds from seeds/seeds.sql...")
	if _, err := pool.Exec(ctx, seedsSQL); err != nil {
		log.Fatalf("[SEED] Failed to execute seeds SQL: %v", err)
	}

	fmt.Println("[SEED] Seeds applied successfully.")
}
