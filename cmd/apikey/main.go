// Command apikey creates an API key for a site and prints it once. Only its
// SHA-256 is stored, so keep the printed key (in the site's config, never in
// git).
//
//	POSTGRES_URL=... go run ./cmd/apikey -site mertmetin -label "WordPress"
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/search"
)

func main() {
	site := flag.String("site", "", "site name, e.g. mertmetin or hotrentals")
	label := flag.String("label", "", "what the key is for")
	flag.Parse()
	if !regexp.MustCompile(`^[a-z][a-z0-9-]{1,30}$`).MatchString(*site) {
		fmt.Fprintln(os.Stderr, "usage: apikey -site <name> [-label text]   (name: lowercase letters, digits, dashes)")
		os.Exit(2)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err == nil {
		_, err = db.Migrate(ctx, pool)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "apikey:", err)
		os.Exit(1)
	}
	key, err := search.Store{Pool: pool}.NewAPIKey(ctx, *site, *label)
	if err != nil {
		fmt.Fprintln(os.Stderr, "apikey:", err)
		os.Exit(1)
	}
	fmt.Println(key)
}
