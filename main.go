package main

import (
	"os"

	"github.com/PierreZ/snap-plugin-publisher-warp10/warp10"
	"github.com/intelsdi-x/snap/control/plugin"
)

func main() {
	meta := warp10.Meta()
	plugin.Start(meta, warp10.NewWarp10Publisher(), os.Args[1])

}
