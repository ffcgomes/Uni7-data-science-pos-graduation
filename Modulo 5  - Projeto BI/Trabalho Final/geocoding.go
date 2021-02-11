package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	"golang.org/x/net/context"
	"googlemaps.github.io/maps"
)

var (

	// Command line paramenters
	sourceFile      = flag.String("sourceFile", "address.csv", "Source file name to be read")
	destinationFile = flag.String("destinationFile", "results.csv", "Destination file name to be written with geocoding information")
	apiKey          = flag.String("key", "", "API Key for using Google Maps API.")
	clientID        = flag.String("client_id", "", "ClientID for Maps for Work API access.")
	signature       = flag.String("signature", "", "Signature for Maps for Work API access.")
	skipFileHeader  = flag.Bool("skipFileHeader", false, "Skip the first file line that correspond to columns headers")
	bounds          = flag.String("bounds", "", "The bounding box of the viewport within which to bias geocode results more prominently.")
	language        = flag.String("language", "", "The language in which to return results.")
	region          = flag.String("region", "", "The region code, specified as a ccTLD two-character value.")
	columnDelimiter = flag.String("columnDelimiter", "\t", "Column delimiter - default is TAB")
	help            = flag.Bool("help", false, "Show this help message")

	//

	mapClient   *maps.Client
	notGeocoded int

	currentID      string
	currentAddress string
	lineNumber     int
)

func main() {
	flag.Parse()

	if *help {
		printUsage()
		return
	}

	initMapClient()
	openFilesAndRunGeocoding(sourceFile, destinationFile)
}

func openFilesAndRunGeocoding(sourceFile *string, destinationFile *string) {
	fileToBeRead, err := os.Open(*sourceFile)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		fmt.Println("To show parameters and software usage, use -help option")
		return
	}
	defer fileToBeRead.Close()

	fileToBeWritten, err := os.Create(*destinationFile)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		fmt.Println("To show parameters and software usage, use -help option")
		return
	}
	defer fileToBeWritten.Close()

	writeOutputHeader(fileToBeWritten)

	iterateOverFileAndGeocode(fileToBeRead, fileToBeWritten)
}

func printUsage() {
	fmt.Println("Geocoding is a tool to geocode files using Google API")
	fmt.Println("www.greenmile.com")
	fmt.Println("")
	fmt.Println("Usage:")
	flag.PrintDefaults()
	fmt.Println("")
	fmt.Println("We expect that source file will containt a structure like this:")
	fmt.Println("")
	fmt.Println("  id, address")
	fmt.Println("Where \"id\" means some sort of identification")
	fmt.Println("and \"address\" is the address to be geocoded")
}

func usageAndExit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	fmt.Println("Flags:")
	flag.PrintDefaults()
	os.Exit(2)
}

func iterateOverFileAndGeocode(fileToBeRead *os.File, fileToBeWritten *os.File) {

	var fileStat, err = fileToBeRead.Stat()
	ifErrorAbort("Error gathering file stats", err)
	var totalFileSize = fileStat.Size()

	scanner := bufio.NewScanner(fileToBeRead)

	progressBar := pb.StartNew(int(totalFileSize))

	lineNumber = 1

	var errors = 0
	var bytesReaded = 0

	for scanner.Scan() {
		line := scanner.Text()
		columns := strings.Split(line, *columnDelimiter)

		currentID = ""
		currentAddress = ""

		if len(columns) < 2 {
			logOutputError(fmt.Sprintf("Expecting two columns, ID and address and found %v", len(columns)), fileToBeWritten)
			errors++
		} else {
			currentID = columns[0]
			currentAddress = columns[1]
			geocodeAndWriteOutput(lineNumber, columns[0], columns[1], fileToBeWritten)
		}

		lineNumber++
		bytesReaded = len(line) + 1 // +1 to Add the removed \n

		progressBar.Add(bytesReaded)
	}

	progressBar.Finish()

	fmt.Printf("Number of lines readed: %v\n", lineNumber)
	fmt.Printf("Number of errors found: %v\n", errors)
	fmt.Printf("Number of lines not geocoded: %v\n", notGeocoded)

}

func geocodeAndWriteOutput(lineNumber int, id string, address string, fileToBeWritten *os.File) {
	r := &maps.GeocodingRequest{
		Address:  address,
		Language: *language,
		Region:   *region,
	}

	resp, err := mapClient.Geocode(context.Background(), r)

	if err != nil {
		if strings.Contains(err.Error(), "OVER_QUERY_LIMIT") {
			for {
				// We reached the query limit... :(
				// There is no way to get it out from here
				// The only option is to wait for the next day
				fmt.Println("We reached the maximum number of requests per day... Waiting for the next hour to try again...")
				fmt.Println(time.Now())
				time.Sleep(time.Hour)

				resp, err = mapClient.Geocode(context.Background(), r)

				if !strings.Contains(err.Error(), "OVER_QUERY_LIMIT") {
					break
				}
			}
		}
	}

	if err != nil {
		logOutputError(err.Error(), fileToBeWritten)
		notGeocoded++
		return
	}

	if len(resp) > 0 {
		logOutputResult(
			resp[0].FormattedAddress,
			resp[0].Geometry.Location.Lat,
			resp[0].Geometry.Location.Lng,
			resp[0].Geometry.LocationType,
			len(resp),
			"",
			fileToBeWritten)
	} else {
		logOutputError("Can't geocode", fileToBeWritten)
		notGeocoded++
	}
}

func writeOutputHeader(fileToBeWritten *os.File) {
	fileToBeWritten.WriteString("Line\tID\tOriginal Address\tFormatted Address\tlat\tlng\tQuality\tNumber of ambigous address\tMesssage\n")
}

func logOutputError(errorMsg string, fileToBeWritten *os.File) {
	logOutputResult(
		"",
		0,
		0,
		"ERROR",
		999,
		errorMsg,
		fileToBeWritten)
}

func logOutputResult(formattedAddress string, lat float64, lng float64, quality string, ambigousAddress int, message string, fileToBeWritten *os.File) {
	s := fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
		lineNumber,
		currentID,
		currentAddress,
		formattedAddress,
		lat,
		lng,
		quality,
		ambigousAddress,
		message)

	fileToBeWritten.WriteString(s)
}

func initMapClient() {
	var err error

	if *apiKey != "" {
		mapClient, err = maps.NewClient(maps.WithAPIKey(*apiKey))
	} else if *clientID != "" || *signature != "" {
		mapClient, err = maps.NewClient(maps.WithClientIDAndSignature(*clientID, *signature))
	} else {
		usageAndExit("Please specify an API Key, or Client ID and Signature.")
	}

	ifErrorAbort("Error initializing map geocoding", err)
}

func ifErrorAbort(msg string, err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s - %s", msg, err)
	}
}
