// +build ignore

package main
import (
	"flag"
	"fmt"
)
func main() {
	var summary bool
	flag.BoolVar(&summary, "summary", false, "Display summary view")
	flag.Parse()
	fmt.Printf("Summary mode: %v\n", summary)
	fmt.Printf("Args: %v\n", flag.Args())
}
