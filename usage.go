package main

import (
    "encoding/json"
    "log"
    "io/ioutil"
    "net/http"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"
)

// Global vars
var SDIR="/var/db/ix-stats"

// What file to store current stats in
var DAILYFILE string

// Create our mutex we use to prevent race conditions when updating
// counters
var wlock sync.Mutex

// Counter for number of increments before a write
var WCOUNTER = 0

//////////////////////////////////////////////////////////
// Tracking JSON Structs
//////////////////////////////////////////////////////////

type t_plat_count struct {
    Name string
    Version string
    Count int
}

type t_service_count struct {
    Name string
    Count int
}

type t_service_share_count struct {
    Name string
    Count int
}

type t_plugin_count struct {
    Name string
    Version string
    Count int
}

type t_pool_vdev_count struct {
    Vdevs int
    Count int
}

type t_pool_disk_count struct {
    Disks int
    Count int
}

type t_pool_type_count struct {
    Type string
    Count int
}

type t_pool_enc_count struct {
    Count int
}

type t_pool_l2arc_count struct {
    Count int
}

type t_pool_zil_count struct {
    Count int
}

type t_pool_capacity_count struct {
    Type string
    Cap int
}

type tracking_json struct {
    // Store Platform Version number count
    Platforms []t_plat_count `json:"platforms"`

    // Store enabled services count
    Services []t_service_count `json:"services"`

    // Store service share counts
    ServiceShares []t_service_share_count `json:"serviceshares"`

    // Store plugin version/number counters
    Plugins []t_plugin_count `json:"plugins"`

    // Store vdev counters for pools
    PoolVdevs []t_pool_vdev_count `json:"poolvdevs"`

    // Store counter of pool disk numbers
    PoolDisks []t_pool_disk_count `json:"pooldisks"`

    // Counter for types of pools
    PoolTypes []t_pool_type_count `json:"pooltype"`

    // Counter for number of pools with encryption
    PoolEnc []t_pool_enc_count `json:"poolenc"`

    // Counter for number of pools with dedicated l2arc
    PoolL2Arc []t_pool_l2arc_count `json:"pooll2arc"`

    // Counter for number of pools with dedicated zil
    PoolL2Zil []t_pool_zil_count `json:"poolzil"`

    // Store the total capacity of globally managed storage
    PoolCapacity []t_pool_capacity_count `json:"poolcapacity"`

    // Total number of system submissions
    SystemCount int
}

var TJSON tracking_json

//////////////////////////////////////////////////////////
// Submission JSON structs
//////////////////////////////////////////////////////////
type s_plugins struct {
    Name string
    Version string
}

type s_pools struct {
    Type string
    Vdevs int
    Disks int
    Capacity int
}

type s_hw struct {
    Cpus int
    Memory int
    Nics int
}

type s_services struct {
    Name string `json:"name"`
    Enabled bool `json:"enabled"`
}

type s_shares struct {
    Type string `json:"type"`
    AllowGuest bool `json:"allowguest"`
}

type submission_json struct {
    Platform string
    Version string
    Plugins []s_plugins `json:"plugins"`
    Pools []s_pools `json:"pools"`
    Hardware s_hw `json:"hardware"`
    Services []s_services `json:"services"`
    Shares []s_shares `json:"shares"`
}

//////////////////////////////////////////////////////////

func increment_pool_types(s submission_json) {
    var found bool
    for j, _ := range s.Pools {
	found = false
        for i, _ := range TJSON.PoolTypes {
	    if ( TJSON.PoolTypes[i].Type == s.Pools[j].Type ) {
                TJSON.PoolTypes[i].Count++
		found = true
                break
             }
         }

        if ( found ) {
		continue
        }

        var newType t_pool_type_count
        newType.Type= s.Pools[j].Type
        newType.Count = 1
        TJSON.PoolTypes = append(TJSON.PoolTypes, newType)
    }
}


func increment_pool_disks(s submission_json) {
    var found bool
    for j, _ := range s.Pools {
	found = false
        for i, _ := range TJSON.PoolDisks {
	    if ( TJSON.PoolDisks[i].Disks == s.Pools[j].Disks ) {
                TJSON.PoolDisks[i].Count++
		found = true
                break
             }
         }

        if ( found ) {
		continue
        }

        var newDisk t_pool_disk_count
        newDisk.Disks= s.Pools[j].Disks
        newDisk.Count = 1
        TJSON.PoolDisks = append(TJSON.PoolDisks, newDisk)
    }
}

func increment_pool_vdev(s submission_json) {
    var found bool
    for j, _ := range s.Pools {
	found = false
        for i, _ := range TJSON.PoolVdevs {
	    if ( TJSON.PoolVdevs[i].Vdevs == s.Pools[j].Vdevs ) {
                TJSON.PoolVdevs[i].Count++
		found = true
                break
             }
         }

        if ( found ) {
		continue
        }

        var newVdev t_pool_vdev_count
        newVdev.Vdevs= s.Pools[j].Vdevs
        newVdev.Count = 1
        TJSON.PoolVdevs = append(TJSON.PoolVdevs, newVdev)
    }
}

func increment_platform(s submission_json) {
    for i, _ := range TJSON.Platforms {
	if ( TJSON.Platforms[i].Name == s.Platform && TJSON.Platforms[i].Version == s.Version ) {
		TJSON.Platforms[i].Count++
		return
	}
    }
    var newPlat t_plat_count
    newPlat.Name = s.Platform
    newPlat.Version = s.Version
    newPlat.Count = 1
    TJSON.Platforms = append(TJSON.Platforms, newPlat)
}

func increment_services(s submission_json) {
    var found bool
    for j, _ := range s.Services {
        found = false
        for i, _ := range TJSON.Services {
	    if ( TJSON.Services[i].Name == s.Services[j].Name) {
		found = true
	        if ( s.Services[j].Enabled ) {
                    TJSON.Services[i].Count++
		}
                break
             }
         }
	 // Found and incremented this particular service
	 if ( found || ! s.Services[j].Enabled ) {
		 continue
	 }
	 var newService t_service_count
         newService.Name = s.Services[j].Name
	 if ( ! s.Services[j].Enabled ) {
             newService.Count = 0
         } else {
             newService.Count = 1
	 }
         TJSON.Services = append(TJSON.Services, newService)
    }
}

func increment_service_shares(s submission_json) {
    var found bool
    for j, _ := range s.Shares {
        found = false
        for i, _ := range TJSON.ServiceShares {
            //log.Println(s.Services[j].Name + " Shares:" + strconv.Itoa(s.Services[j].Shares))
	    if ( TJSON.ServiceShares[i].Name == s.Shares[j].Type ) {
                TJSON.ServiceShares[i].Count++
		found = true
                break
             }
         }
	 // Found and incremented this particular service
	 if ( found ) {
		 continue
	 }
	 var newService t_service_share_count
         newService.Name = s.Shares[j].Type
         newService.Count = 1
         TJSON.ServiceShares = append(TJSON.ServiceShares, newService)
    }
}

func parse_data(s submission_json) {

    // Do this all within a locked mutex
    wlock.Lock()

    // Check if the daily file needs to roll over
    get_daily_filename()

    // Increase total number of systems
    TJSON.SystemCount++

    // Update our in-memory counters
    increment_platform(s)
    increment_services(s)
    increment_service_shares(s)
    increment_pool_vdev(s)
    increment_pool_disks(s)
    increment_pool_types(s)

    // TODO increment other submitted counters
    log.Println(s.Plugins)
    log.Println(s.Pools)
    log.Println(s.Hardware)

    // Every 5 updates, we update the JSON file on disk
    if ( WCOUNTER >= 5 ) {
	log.Println("Flushing to disk")
        flush_json_to_disk()
	WCOUNTER=0
    } else {
        WCOUNTER++
    }

    //log.Println(TJSON)

    // Unlock the mutex now
    wlock.Unlock()
}

// Getting a new submission
func submit(rw http.ResponseWriter, req *http.Request) {
    decoder := json.NewDecoder(req.Body)

    // Decode the POST data json struct
    var s submission_json
    err := decoder.Decode(&s)
    if err != nil {
	log.Println(err)
        return
    }

    // Do things with the data
    parse_data(s)
}

// Clear out the JSON structure counters
func zero_out_stats() {
	TJSON = tracking_json{}
}

// Get the latest daily file to store data
func get_daily_filename() {
	t := time.Now()
	newfile := SDIR + "/" + t.Format("20060102") + ".json"
	if ( newfile != DAILYFILE ) {

	    // Flush previous data to disk
	    if ( DAILYFILE != "" ) {
		flush_json_to_disk()
	    }
	    // Timestamp has changed, lets reset our in-memory json counters structure
	    zero_out_stats()

	    // Set new DAILYFILE
	    DAILYFILE = newfile
	}

}

// Load the daily file into memory
func load_daily_file() {
    get_daily_filename()

    // No file yet? Lets clear out the struct
    if _, err := os.Stat(DAILYFILE) ; os.IsNotExist(err) {
	zero_out_stats()
        return
    }

    // Load the file into memory
    dat, err := ioutil.ReadFile(DAILYFILE)
    if ( err != nil ) {
	log.Println(err)
        log.Fatal("Failed loading daily file: " + DAILYFILE )
    }
    if err := json.Unmarshal(dat, &TJSON); err != nil {
	log.Println(err)
        log.Fatal("Failed unmarshal of JSON in DAILYFILE:")
    }
}

func flush_json_to_disk() {
    file, _ := json.MarshalIndent(TJSON, "", " ")
    _ = ioutil.WriteFile(DAILYFILE, file, 0644)
}

// Lets do it!
func main() {

    // Capture SIGTERM and flush JSON to disk
    var gracefulStop = make(chan os.Signal)
    signal.Notify(gracefulStop, syscall.SIGTERM)
    signal.Notify(gracefulStop, syscall.SIGINT)
    go func() {
        sig := <-gracefulStop
	log.Println("%v", sig)
	log.Println("Caught Signal. Flushing JSON to disk")
	flush_json_to_disk()
        os.Exit(0)
    }()

    // Read the daily file into memory at startup
    load_daily_file()

    // Start our HTTP listener
    http.HandleFunc("/submit", submit)
    log.Fatal(http.ListenAndServe(":8082", nil))
}
