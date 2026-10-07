package web

import "embed"

//go:embed index.html
var StaticFS embed.FS
