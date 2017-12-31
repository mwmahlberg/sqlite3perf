// Copyright © 2017 Markus W Mahlberg <markus@mahlberg.io>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
)

var numRecs int
var vacuum bool
var logSeconds int

// generateCmd represents the generate command
var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "generate records to benchmark against",
	Long: `This command generates records to benchmark against.
Each record consists of an ID, a 8 byte hex encoded random value
and a SHA256 hash of said random value.

ATTENTION: The 'bench' table will be DROPPED each time this command is called, before it
is (re)-generated!
	`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf("Generating %d records", numRecs)

		log.Println("Opening database")
		// Database Setup
		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			log.Fatalf("Error while opening database '%s': %s", dbPath, err.Error())
		}
		defer db.Close()

		log.Println("Dropping table 'bench' if already present")
		if _, err := db.Exec("DROP TABLE IF EXISTS bench"); err != nil {
			log.Fatalf("Could not delete table 'bench' for (re-)generation of data: %s", err)
		}

		log.Println("(Re-)creating table 'bench'")
		if _, err := db.Exec("CREATE TABLE bench (ID int PRIMARY KEY ASC, rand TEXT, hash TEXT);"); err != nil {
			log.Fatalf("Could not create table 'bench': %s", err)
		}

		log.Println("Setting up the environment")
		// Prepare values needed so that there aren't any allocations done in the loop
		st, _ := db.Prepare("INSERT INTO bench VALUES(?,?,?);")

		// Precalculate the percentage each record represents
		p := float64(100) / float64(numRecs)

		// Initialize the hasher once and reuse it using Reset()
		h := sha256.New()

		// We use a 8 byte random value as this is the optimal size for SHA256,
		// which operates on 64bit blocks
		b := make([]byte, 8)

		// Preinitialize i so that we can use it in a goroutine to give proper feedback
		var i int
		l := len(fmt.Sprintf("%d", numRecs))

		// Set up logging mechanism. We use a goroutine here which logs the
		// records already generated every two seconds until "done" is signaled
		// via the channel.
		done := make(chan bool)

		start := time.Now()

		go func() {
			log.Println("Starting progress logging")
			ticker := time.NewTicker(time.Second * 2).C
			for {
				select {
				// Since this is a time consuming process depending on the number of
				// records	created, we want some feedback every 2 seconds
				case <-ticker:
					log.Printf("%*d/%*d (%6.2f%%) written, avg: %s/record, %2.2f records/s",
						l, i, l, numRecs, p*float64(i),
						time.Duration(time.Since(start).Nanoseconds()/int64(i)), float64(i)/time.Since(start).Seconds())
				case <-done:
					return
				}
			}

		}()

		// Start generation of actual records
		log.Println("Starting inserts")
		for i = 0; i < numRecs; i++ {

			if _, err := rand.Read(b); err != nil {
				log.Fatalf("Can not read random values: %s", err)
			}

			// Fill the hasher
			h.Write(b)

			if _, err := st.Exec(i, hex.EncodeToString(b), hex.EncodeToString(h.Sum(nil))); err != nil {
				log.Fatalf("Inserting values into database failed: %s", err)
			}

			// Reset the hasher so we can reuse it
			h.Reset()

		}

		stop := time.Now()

		// Signal the progress log that we are done
		done <- true

		dur := time.Duration(stop.Sub(start))

		log.Printf("%*d/%*d (%6.2f%%) written in %s", l, i, l, numRecs, p*float64(i), stop.Sub(start))
		log.Printf("Average time per Record: %s, %2.2f records/s\n", time.Duration(dur.Nanoseconds()/int64(numRecs)), float64(numRecs)/dur.Seconds())

		if vacuum {
			log.Println("Vaccumating database file")
			start = time.Now()

			if _, err := db.Exec("VACUUM;"); err != nil {
				log.Printf("Vacuumating database caused an error: %s", err)
				log.Println("Proceed with according caution.")
			}

			log.Printf("Vacuumation took %s", time.Since(start))
		}
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)

	// Here you will define your flags and configuration settings.

	generateCmd.Flags().IntVarP(&numRecs, "records", "r", 1000, "number of records to generate")
	generateCmd.Flags().IntVarP(&logSeconds, "interval", "i", 2, "interval between progress messages")
	generateCmd.Flags().BoolVarP(&vacuum, "vacuum", "v", false, "VACUUM database file after the records were generated.")

}
